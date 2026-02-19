from sqlalchemy import Boolean, Column, DateTime, Float, ForeignKey, Integer, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from .database import Base


class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(Text, nullable=False)
    uploaded_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    total_jobs = Column(Integer, nullable=False, default=0)

    role_column = Column(Text, nullable=True)
    country_column = Column(Text, nullable=True)
    skills_column = Column(Text, nullable=True)
    description_column = Column(Text, nullable=True)
    date_column = Column(Text, nullable=True)

    has_skills_column = Column(Boolean, nullable=False, default=False)
    used_description_extraction = Column(Boolean, nullable=False, default=False)
    has_date_column = Column(Boolean, nullable=False, default=False)

    mapping_confidence = Column(Float, nullable=False, default=0.0)

    jobs = relationship("JobRecord", back_populates="dataset", cascade="all, delete-orphan")


class JobRecord(Base):
    __tablename__ = "job_records"

    id = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id"), nullable=False, index=True)

    role = Column(Text, nullable=True, index=True)
    country = Column(Text, nullable=True, index=True)
    skills_text = Column(Text, nullable=True)
    description = Column(Text, nullable=True)
    posted_date = Column(DateTime(timezone=False), nullable=True, index=True)
    raw_json = Column(Text, nullable=True)

    dataset = relationship("Dataset", back_populates="jobs")


class AgentRun(Base):
    __tablename__ = "agent_runs"

    id = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id"), nullable=True, index=True)

    query = Column(Text, nullable=False)
    parsed_intent = Column(Text, nullable=True)
    parsed_filters = Column(Text, nullable=True)
    execution_plan = Column(Text, nullable=True)
    output_summary = Column(Text, nullable=True)

    status = Column(Text, nullable=False, default="completed")
    confidence = Column(Float, nullable=False, default=0.0)
    warnings = Column(Text, nullable=True)

    started_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    finished_at = Column(DateTime(timezone=True), nullable=True)
