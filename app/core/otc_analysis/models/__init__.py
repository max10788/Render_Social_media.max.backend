# Existing imports
from app.core.otc_analysis.models.wallet import OTCWallet, Wallet
from app.core.otc_analysis.models.watchlist import WatchlistItem
from app.core.otc_analysis.models.alert import Alert

# ✅ NEU: Auto-Migration importieren (läuft automatisch beim Import)
import app.core.otc_analysis.models.auto_migrate

__all__ = ['OTCWallet', 'Wallet', 'WatchlistItem', 'Alert']
