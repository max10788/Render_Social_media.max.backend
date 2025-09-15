# app/core/backend_crypto_tracker/scanner/low_cap_analyzer.py
"""
Low-Cap-Token-Analysator, der verschiedene Analysekomponenten integriert.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
import asyncio
import logging

from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.utils.exceptions import (
    APIException,
    NotFoundException,
    CustomAnalysisException,
    ValidationException
)

# Importiere die Klassen aus den bestehenden Dateien
# TokenAnalyzer aus token_analyzer.py
from app.core.backend_crypto_tracker.scanner.token_analyzer import (
    TokenAnalyzer,
    TokenAnalysisConfig
)

# Wallet-Komponenten aus wallet_classifier.py
# Beachte: Die Hauptklasse heißt EnhancedWalletClassifier, aber wir importieren sie als WalletClassifier
# WalletTypeEnum und WalletAnalysis sind auch dort definiert
from app.core.backend_crypto_tracker.scanner.wallet_classifier import (
    EnhancedWalletClassifier as WalletClassifier,
    WalletTypeEnum,
    WalletAnalysis
)

# RiskAssessment aus risk_assessor.py
from app.core.backend_crypto_tracker.scanner.risk_assessor import (
    AdvancedRiskAssessor,
    RiskAssessment
)

# Scoring-Komponenten aus scoring_engine.py
from app.core.backend_crypto_tracker.scanner.scoring_engine import (
    MultiChainScoringEngine,
    ScanConfig
)

# Token-Modell (wird in token_analyzer.py verwendet)
# from app.core.backend_crypto_tracker.processor.database.models.token import Token


class LowCapAnalyzer:
    """
    Zentrale Fassade/Koordinator für die gesamte Low-Cap-Token-Analyse.
    Integriert Funktionalitäten aus verschiedenen Analysekomponenten.
    """
    def __init__(
        self,
        config: Optional[TokenAnalysisConfig] = None,
        scan_config: Optional[ScanConfig] = None
    ):
        self.logger = get_logger(__name__)
        self.config = config or TokenAnalysisConfig()
        self.scan_config = scan_config or ScanConfig()
        
        # Initialisiere die Komponenten (Ressourcen werden in __aenter__ erstellt)
        self.token_analyzer: Optional[TokenAnalyzer] = None
        self.risk_assessor: Optional[AdvancedRiskAssessor] = None
        self.scoring_engine: Optional[MultiChainScoringEngine] = None
        self.wallet_classifier: Optional[WalletClassifier] = None
        
        self.logger.info("LowCapAnalyzer initialisiert")

    async def __aenter__(self):
        """Initialisiert asynchrone Ressourcen"""
        self.logger.info("Initialisiere asynchrone Ressourcen für LowCapAnalyzer")
        try:
            # Erstelle Instanzen der Komponenten mit asynchroner Initialisierung
            self.token_analyzer = TokenAnalyzer(self.config)
            await self.token_analyzer.__aenter__()
            
            self.risk_assessor = AdvancedRiskAssessor()
            self.scoring_engine = MultiChainScoringEngine()
            self.wallet_classifier = WalletClassifier()
            await self.wallet_classifier.__aenter__()
            
            self.logger.info("Asynchrone Ressourcen erfolgreich initialisiert")
            return self
        except Exception as e:
            self.logger.error(f"Fehler bei der Initialisierung der Ressourcen: {str(e)}")
            await self.__aexit__(type(e), e, e.__traceback__)
            raise CustomAnalysisException(f"Initialisierung fehlgeschlagen: {str(e)}")

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Schließt asynchrone Ressourcen"""
        self.logger.info("Schließe asynchrone Ressourcen für LowCapAnalyzer")
        
        # Schließe alle Komponenten in umgekehrter Reihenfolge der Erstellung
        close_tasks = []
        
        if self.wallet_classifier and hasattr(self.wallet_classifier, '__aexit__'):
            close_tasks.append(self._safe_close_component(
                self.wallet_classifier, exc_type, exc_val, exc_tb, "wallet_classifier"))
        
        if self.token_analyzer and hasattr(self.token_analyzer, '__aexit__'):
            close_tasks.append(self._safe_close_component(
                self.token_analyzer, exc_type, exc_val, exc_tb, "token_analyzer"))
        
        # Alle Schließvorgänge parallel ausführen
        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)
        
        self.logger.info("Asynchrone Ressourcen erfolgreich geschlossen")

    async def _safe_close_component(self, component, exc_type, exc_val, exc_tb, component_name):
        """Sicheres Schließen einer Komponente"""
        try:
            await component.__aexit__(exc_type, exc_val, exc_tb)
        except Exception as e:
            self.logger.warning(f"Fehler beim Schließen von {component_name}: {str(e)}")

    async def analyze_custom_token(self, token_address: str, chain: str) -> Dict[str, Any]:
        """Zentrale Analyse-Methode für einen einzelnen Token"""
        self.logger.info(f"Starte Analyse für Token {token_address} auf Chain {chain}")
        
        # Validierung der Eingabeparameter
        if not token_address or not isinstance(token_address, str) or not token_address.strip():
            error_msg = "Token-Adresse muss ein nicht-leerer String sein"
            self.logger.error(error_msg)
            raise ValidationException(error_msg, field="token_address")
        
        if not chain or not isinstance(chain, str) or not chain.strip():
            error_msg = "Chain muss ein nicht-leerer String sein"
            self.logger.error(error_msg)
            raise ValidationException(error_msg, field="chain")
        
        # Normalisiere Chain-Name (kleinschreibung)
        chain = chain.lower().strip()
        
        # Prüfe, ob der TokenAnalyzer initialisiert ist
        if not self.token_analyzer:
            error_msg = "TokenAnalyzer ist nicht initialisiert. Verwenden Sie den Analyzer innerhalb eines async-Kontext-Managers (async with)."
            self.logger.error(error_msg)
            raise CustomAnalysisException(error_msg)
        
        try:
            # Delegiere die Analyse an den TokenAnalyzer
            result = await self.token_analyzer.analyze_custom_token(token_address, chain)
            self.logger.info(f"Analyse für Token {token_address} auf Chain {chain} abgeschlossen")
            return result
        except ValueError as e:
            # Spezielle Behandlung für "Token data could not be retrieved" Fehler
            if "Token data could not be retrieved" in str(e):
                self.logger.error(f"Konnte Tokendaten nicht abrufen für {token_address} auf {chain}: {str(e)}")
                raise CustomAnalysisException(
                    "Tokendaten konnten nicht abgerufen werden. Bitte überprüfen Sie die Token-Adresse oder versuchen Sie es später erneut."
                ) from e
            raise CustomAnalysisException(f"Analyse fehlgeschlagen: {str(e)}") from e
        except (APIException, NotFoundException) as e:
            self.logger.error(f"Externer Fehler bei der Token-Analyse: {str(e)}")
            raise CustomAnalysisException(f"Analyse fehlgeschlagen: {str(e)}") from e
        except Exception as e:
            self.logger.error(f"Unerwarteter Fehler bei der Token-Analyse: {str(e)}", exc_info=True)
            raise CustomAnalysisException(f"Unerwarteter Fehler bei der Analyse: {str(e)}") from e

    async def scan_low_cap_tokens(self, max_tokens: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Massenanalyse-Methode für mehrere Tokens (z.B. für periodische Scans).
        Diese Methode delegiert die Arbeit an den bereits instanziierten TokenAnalyzer.

        Args:
            max_tokens: Maximale Anzahl von Tokens, die analysiert werden sollen

        Returns:
            Eine Liste von Dictionaries mit Analyseergebnissen für jeden Token
        """
        self.logger.info(f"Starte Low-Cap-Token-Scan mit max_tokens={max_tokens}")

        # Prüfe, ob der TokenAnalyzer initialisiert ist
        if not self.token_analyzer:
            error_msg = "TokenAnalyzer ist nicht initialisiert. Verwenden Sie den Analyzer innerhalb eines async-Kontext-Managers (async with)."
            self.logger.error(error_msg)
            raise CustomAnalysisException(error_msg)

        try:
            # Delegiere den Scan an den TokenAnalyzer
            # TokenAnalyzer.scan_low_cap_tokens erwartet max_tokens als int oder None
            results = await self.token_analyzer.scan_low_cap_tokens(max_tokens)

            self.logger.info(f"Low-Cap-Token-Scan abgeschlossen. {len(results)} Tokens erfolgreich analysiert")
            return results

        except Exception as e:
            self.logger.error(f"Fehler beim Low-Cap-Token-Scan: {str(e)}", exc_info=True)
            raise CustomAnalysisException(f"Scan fehlgeschlagen: {str(e)}") from e

    # Optional: Erweiterte Analysemethoden, die die anderen Komponenten nutzen
    # Diese könnten verwendet werden, um zusätzliche Analysen durchzuführen,
    # die über die Standardfunktionalität des TokenAnalyzers hinausgehen.
    
    async def _perform_advanced_risk_assessment(self, token_data: Dict[str, Any], wallet_analyses: List[WalletAnalysis]) -> RiskAssessment:
        """
        Führt eine erweiterte Risikobewertung mit dem AdvancedRiskAssessor durch.
        
        Args:
            token_data: Token-Daten aus der ersten Analyse
            wallet_analyses: Ergebnisse der Wallet-Analyse
            
        Returns:
            RiskAssessment mit detaillierten Risikoinformationen
        """
        if not self.risk_assessor:
            raise CustomAnalysisException("RiskAssessor ist nicht initialisiert")
            
        try:
            # Verwende die Methode aus AdvancedRiskAssessor
            # Achtung: Die Signatur muss mit der in risk_assessor.py übereinstimmen
            # Dort ist es: async def assess_token_risk_advanced(self, token_data: Dict[str, Any], wallet_analyses: List, transaction_history: List[Dict] = None)
            return await self.risk_assessor.assess_token_risk_advanced(token_data, wallet_analyses)
        except Exception as e:
            self.logger.error(f"Fehler bei der erweiterten Risikobewertung: {str(e)}")
            # Fallback auf eine Basisbewertung
            return await self.risk_assessor.assess_token_risk(token_data, wallet_analyses)
    
    async def _calculate_advanced_score(self, token_data: Dict[str, Any], wallet_analyses: List[WalletAnalysis], chain: str) -> Dict[str, Any]:
        """
        Berechnet einen erweiterten Score mit dem MultiChainScoringEngine.
        
        Args:
            token_data: Token-Daten
            wallet_analyses: Ergebnisse der Wallet-Analyse
            chain: Blockchain
            
        Returns:
            Dictionary mit Scoring-Ergebnissen
        """
        if not self.scoring_engine:
            raise CustomAnalysisException("ScoringEngine ist nicht initialisiert")
            
        try:
            # Verwende die Methode aus MultiChainScoringEngine
            # Achtung: Die Signatur muss mit der in scoring_engine.py übereinstimmen
            # Dort ist es: def calculate_token_score_custom(self, token_data, wallet_analyses: List, chain: str)
            return self.scoring_engine.calculate_token_score_custom(token_data, wallet_analyses, chain)
        except Exception as e:
            self.logger.error(f"Fehler bei der erweiterten Scoring-Berechnung: {str(e)}")
            # Fallback auf ein einfaches Scoring
            return {"total_score": 50.0, "metrics": {}, "risk_flags": ["scoring_failed"]}

# Alias für Kompatibilität, falls andere Module "LowCapAnalyzer" erwarten
# LowCapAnalyzer = LowCapAnalyzer # Dies ist redundant, aber zeigt die Absicht
