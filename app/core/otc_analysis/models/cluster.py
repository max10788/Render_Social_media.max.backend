from sqlalchemy import Column, String, Float, DateTime, Integer, JSON, Text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class Cluster(Base):
    __tablename__ = 'clusters'
    
    # Primary identifier
    cluster_id = Column(String(66), primary_key=True)  # Hash of seed addresses
    
    # Cluster metadata
    cluster_type = Column(String(50), index=True)  # 'otc_network', 'exchange_cluster', 'entity_cluster'
    name = Column(String(255), nullable=True)
    description = Column(Text, nullable=True)
    
    # Composition
    wallet_count = Column(Integer, default=0)
    seed_addresses = Column(JSON, default=list)  # Original addresses that formed cluster
    member_addresses = Column(JSON, default=list)  # All addresses in cluster
    
    # Activity metrics
    first_activity = Column(DateTime)
    last_activity = Column(DateTime)
    total_transactions = Column(Integer, default=0)
    total_volume_usd = Column(Float, default=0.0)
    
    # Network metrics
    avg_betweenness = Column(Float, default=0.0)
    avg_degree = Column(Float, default=0.0)
    cluster_density = Column(Float, default=0.0)
    modularity_score = Column(Float, default=0.0)
    
    # Behavioral patterns
    topology_type = Column(String(50))  # 'hub_spoke', 'mesh', 'chain'
    hub_addresses = Column(JSON, default=list)  # Central nodes in cluster
    
    # OTC-specific
    otc_confidence = Column(Float, default=0.0, index=True)
    suspected_entity = Column(String(255), nullable=True)  # e.g., "Wintermute Network"
    
    # Temporal patterns
    active_time_windows = Column(JSON, default=list)  # [{start: hour, end: hour}]
    peak_activity_hours = Column(JSON, default=list)
    
    # Metadata
    algorithm_version = Column(String(20))  # Track which clustering algo was used
    similarity_threshold = Column(Float)
    last_updated = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f"<Cluster {self.cluster_id[:10]}... {self.wallet_count} wallets, confidence:{self.otc_confidence:.2f}>"
