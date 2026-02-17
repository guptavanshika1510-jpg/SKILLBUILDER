from sqlalchemy import Column, Integer, String, Text, Date, Float
from .database import Base

class JobPosting(Base):
    __tablename__ = "job_postings"

    id = Column(Integer, primary_key=True, index=True)

    job_title = Column(String, default="")
    company = Column(String, default="")
    location = Column(String, default="")
    country = Column(String, default="Unknown")

    dataset_role = Column(String, default="Unknown")
    role_category = Column(String, default="")

    description = Column(Text, default="")

    posted_date = Column(Date, nullable=True)

    salary_min = Column(Float, nullable=True)
    salary_max = Column(Float, nullable=True)

    extracted_skills = Column(Text, default="")  # stored as comma-separated
