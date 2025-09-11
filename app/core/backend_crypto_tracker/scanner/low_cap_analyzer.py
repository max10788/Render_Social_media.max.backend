"""
Low-Cap-Token-Analysator, der verschiedene Analysekomponenten integriert.
"""
from typing import Dict, List, Optional, Any
from datetime import datetime
import asyncio

from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.utils.exceptions import (
    APIException, 
    NotFoundException, 
    CustomAnalysisException,
    ValidationException
)

# Importiere die Klassen aus den bestehenden Dateien
from app.core.backend_crypto_tracker.scanner.token_analyzer import (
    TokenAnalyzer, 
    TokenAnalysisConfig
)
from app.core.backend_crypto_tracker.scanner.risk_assessor import (
    AdvancedRiskAssessor, 
    RiskAssessment
)
from app.core.backend_crypto_tracker.scanner.scoring_engine import (
    MultiChainScoringEngine, 
    ScanConfig
)
from app.core.backend_crypto_tracker.scanner.wallet_classifier import (
    WalletClassifier, 
    WalletTypeEnum, 
    WalletAnalysis
)


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
        """
        Initialisiert die benötigten Komponenten.
        
        Args:
            config: Konfiguration für die Token-Analyse
            scan_config: Konfiguration für den Scan-Vorgang
        """
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
        """
        Initialisiert asynchrone Ressourcen (z.B. API-Clients) der untergeordneten Komponenten.
        """
        self.logger.info("Initialisiere asynchrone Ressourcen für LowCapAnalyzer")
        
        try:
            # Erstelle Instanzen der Komponenten mit asynchroner Initialisierung
            self.token_analyzer = TokenAnalyzer(self.config)
            await self.token_analyzer.__aenter__()
            
            self.risk_assessor = AdvancedRiskAssessor()
            await self.risk_assessor.__aenter__()
            
            self.scoring_engine = MultiChainScoringEngine()
            await self.scoring_engine.__aenter__()
            
            self.wallet_classifier = WalletClassifier()
            await self.wallet_classifier.__aenter__()
            
            self.logger.info("Asynchrone Ressourcen erfolgreich initialisiert")
            return self
            
        except Exception as e:
            self.logger.error(f"Fehler bei der Initialisierung der Ressourcen: {str(e)}")
            raise CustomAnalysisException(f"Initialisierung fehlgeschlagen: {str(e)}")
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Schließt asynchrone Ressourcen der untergeordneten Komponenten.
        """
        self.logger.info("Schließe asynchrone Ressourcen für LowCapAnalyzer")
        
        # Schließe alle Komponenten in umgekehrter Reihenfolge
        if self.wallet_classifier:
            await self.wallet_classifier.__aexit__(exc_type, exc_val, exc_tb)
            
        if self.scoring_engine:
            await self.scoring_engine.__aexit__(exc_type, exc_val, exc_tb)
            
        if self.risk_assessor:
            await self.risk_assessor.__aexit__(exc_type, exc_val, exc_tb)
            
        if self.token_analyzer:
            await self.token_analyzer.__aexit__(exc_type, exc_val, exc_tb)
            
        self.logger.info("Asynchrone Ressourcen erfolgreich geschlossen")
    
    async def analyze_custom_token(self, token_address: str, chain: str) -> Dict[str, Any]:
        """
        Zentrale Analyse-Methode für einen einzelnen Token.
        
        Args:
            token_address: Die Adresse des zu analysierenden Tokens
            chain: Die Blockchain, auf der der Token gehandelt wird
            
        Returns:
            Ein Dictionary mit allen Analyseergebnissen
            
        Raises:
            ValidationException: Bei ungültigen Eingabeparametern
            CustomAnalysisException: Bei Fehlern während der Analyse
        """
        self.logger.info(f"Starte Analyse für Token {token_address} auf Chain {chain}")
        
        # Validierung der Eingabeparameter
        if not token_address or not chain:
            error_msg = "Token-Adresse und Chain müssen angegeben werden"
            self.logger.error(error_msg)
            raise ValidationException(error_msg)
        
        try:
            # 1. Datenbeschaffung
            token_data = await self.token_analyzer._fetch_custom_token_data(token_address, chain)
            holders = await self.token_analyzer._fetch_token_holders(token_address, chain)
            
            # 2. Wallet-Klassifizierung für alle Holder
            wallet_analyses = []
            for holder in holders:
                wallet_address = holder.get('address')
                if wallet_address:
                    wallet_analysis = await self.wallet_classifier.classify_wallet(wallet_address, chain)
                    wallet_analyses.append(wallet_analysis)
            
            # 3. Transaktionsdaten für die Top-Wallets abrufen
            top_wallets = [holder.get('address') for holder in holders[:10]]  # Top 10 Holder
            transaction_data = await self.token_analyzer._fetch_wallet_transaction_data(top_wallets, chain)
            
            # 4. Risikobewertung
            risk_assessment = await self.risk_assessor.assess_token_risk_advanced(
                token_address, 
                chain, 
                holders_data=holders,
                transaction_data=transaction_data
            )
            
            # 5. Scoring
            scoring_result = await self.scoring_engine.calculate_token_score_advanced(
                token_address,
                chain,
                token_data=token_data,
                holders_data=holders,
                risk_assessment=risk_assessment
            )
            
            # 6. Ergebnisse zusammenstellen
            result = self._integrate_results(
                token_data=token_data,
                wallet_analyses=wallet_analyses,
                risk_assessment=risk_assessment,
                scoring_result=scoring_result
            )
            
            self.logger.info(f"Analyse für Token {token_address} abgeschlossen")
            return result
            
        except (APIException, NotFoundException) as e:
            self.logger.error(f"Externer Fehler bei der Token-Analyse: {str(e)}")
            raise CustomAnalysisException(f"Analyse fehlgeschlagen: {str(e)}")
        except Exception as e:
            self.logger.error(f"Unerwarteter Fehler bei der Token-Analyse: {str(e)}", exc_info=True)
            raise CustomAnalysisException(f"Unerwarteter Fehler bei der Analyse: {str(e)}")
    
    async def scan_low_cap_tokens(self, max_tokens: int = None) -> List[Dict[str, Any]]:
        """
        Massenanalyse-Methode für mehrere Tokens (z.B. für periodische Scans).
        
        Args:
            max_tokens: Maximale Anzahl von Tokens, die analysiert werden sollen
            
        Returns:
            Eine Liste von Dictionaries mit Analyseergebnissen für jeden Token
        """
        self.logger.info(f"Starte Low-Cap-Token-Scan mit max_tokens={max_tokens}")
        
        # Konfiguration anpassen
        scan_config = self.scan_config
        if max_tokens:
            scan_config.max_tokens = max_tokens
        
        try:
            # 1. Token-Liste abrufen
            tokens_to_scan = await self.token_analyzer.scan_low_cap_tokens(scan_config)
            
            # 2. Analyse für jeden Token durchführen
            analysis_tasks = []
            for token in tokens_to_scan:
                token_address = token.get('address')
                chain = token.get('chain')
                if token_address and chain:
                    task = self.analyze_custom_token(token_address, chain)
                    analysis_tasks.append(task)
            
            # 3. Alle Analysen parallel ausführen
            results = await asyncio.gather(*analysis_tasks, return_exceptions=True)
            
            # 4. Ergebnisse verarbeiten und Exceptions filtern
            successful_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    token_address = tokens_to_scan[i].get('address')
                    self.logger.error(f"Fehler bei der Analyse von Token {token_address}: {str(result)}")
                else:
                    successful_results.append(result)
            
            # 5. Ergebnisse nach Score sortieren
            successful_results.sort(key=lambda x: x.get('scoring', {}).get('total_score', 0), reverse=True)
            
            self.logger.info(f"Low-Cap-Token-Scan abgeschlossen. {len(successful_results)} Tokens erfolgreich analysiert")
            return successful_results
            
        except Exception as e:
            self.logger.error(f"Fehler beim Low-Cap-Token-Scan: {str(e)}", exc_info=True)
            raise CustomAnalysisException(f"Scan fehlgeschlagen: {str(e)}")
    
    def _integrate_results(
        self, 
        token_data: Dict[str, Any],
        wallet_analyses: List[WalletAnalysis],
        risk_assessment: RiskAssessment,
        scoring_result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Hilfsmethode, die die Ergebnisse der verschiedenen Komponenten in ein einheitliches Ausgabeformat kombiniert.
        
        Args:
            token_data: Grundlegende Token-Daten
            wallet_analyses: Ergebnisse der Wallet-Klassifizierung
            risk_assessment: Ergebnis der Risikobewertung
            scoring_result: Ergebnis des Scoring
            
        Returns:
            Ein kombiniertes Dictionary mit allen Analyseergebnissen
        """
        self.logger.debug("Integriere Analyseergebnisse")
        
        # Wallet-Analysen für die Ausgabe aufbereiten
        wallet_summary = {
            'total_holders': len(wallet_analyses),
            'wallet_types': {
                'exchange': 0,
                'whale': 0,
                'retail': 0,
                'team': 0,
                'contract': 0,
                'unknown': 0
            }
        }
        
        for wallet in wallet_analyses:
            wallet_type = wallet.wallet_type.value if wallet.wallet_type else 'unknown'
            if wallet_type in wallet_summary['wallet_types']:
                wallet_summary['wallet_types'][wallet_type] += 1
        
        # Kombiniere alle Ergebnisse
        integrated_result = {
            'metadata': {
                'timestamp': datetime.now().isoformat(),
                'analyzer_version': '1.0.0'
            },
            'token': token_data,
            'wallet_analysis': wallet_summary,
            'risk': risk_assessment.__dict__ if risk_assessment else {},
            'scoring': scoring_result,
            'summary': {
                'risk_level': risk_assessment.risk_level if risk_assessment else 'unknown',
                'total_score': scoring_result.get('total_score', 0),
                'recommendation': self._generate_recommendation(
                    risk_assessment.risk_level if risk_assessment else 'unknown',
                    scoring_result.get('total_score', 0)
                )
            }
        }
        
        return integrated_result
    
    def _generate_recommendation(self, risk_level: str, total_score: float) -> str:
        """
        Generiert eine Empfehlung basierend auf Risikolevel und Score.
        
        Args:
            risk_level: Das ermittelte Risikolevel
            total_score: Der ermittelte Gesamtscore
            
        Returns:
            Eine textuelle Empfehlung
        """
        if risk_level == 'low' and total_score >= 70:
            return "Strong Buy"
        elif risk_level in ['low', 'medium'] and total_score >= 50:
            return "Buy"
        elif risk_level == 'medium' and total_score >= 30:
            return "Hold"
        elif risk_level == 'high' or total_score < 30:
            return "Avoid"
        else:
            return "Neutral"
