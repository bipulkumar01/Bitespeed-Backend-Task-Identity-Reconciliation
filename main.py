from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr, Field, root_validator
from sqlalchemy import create_engine, Column, Integer, String, DateTime, or_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import logging
from typing import Optional

DATABASE_URL = "postgresql://postgres:123456@localhost/dbname"

# SQLAlchemy setup
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database model
class Contact(Base):
    __tablename__ = "contacts"
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, index=True, nullable=True)
    phone_number = Column(String, index=True, nullable=True)
    linked_id = Column(Integer, nullable=True)
    link_precedence = Column(String, nullable=False, default="primary")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)
    deleted_at = Column(DateTime, nullable=True)

Base.metadata.create_all(bind=engine)

# Pydantic model for request validation
class ContactIn(BaseModel):
    email: Optional[EmailStr] = Field(None, description="Email address of the contact")
    phone_number: Optional[str] = Field(None, description="Phone number of the contact")

    @staticmethod
    def validate_email_or_phone_number(email, phone_number):
        if email is None and phone_number is None:
            raise ValueError("At least one of email or phone_number must be provided.")

    @root_validator(pre=True)
    def check_email_or_phone_number(cls, values):
        email = values.get('email')
        phone_number = values.get('phone_number')
        cls.validate_email_or_phone_number(email, phone_number)
        return values

@app.post("/identify")
async def identify(contact_in: ContactIn):
    session = SessionLocal()
    try:
        # Retrieve existing contacts based on email or phone number
        existing_contacts = session.query(Contact).filter(
            or_(
                Contact.email == contact_in.email, 
                Contact.phone_number == contact_in.phone_number
            )
        ).all()

        if not existing_contacts:
            # If no existing contact is found, create a new primary contact
            new_contact = Contact(
                email=contact_in.email,
                phone_number=contact_in.phone_number,
                link_precedence="primary"
            )
            session.add(new_contact)
            session.commit()
            session.refresh(new_contact)
            return {
                "contact": {
                    "primaryContactId": new_contact.id,
                    "emails": [new_contact.email] if new_contact.email else [],
                    "phoneNumbers": [new_contact.phone_number] if new_contact.phone_number else [],
                    "secondaryContactIds": []
                }
            }

        # Identify the primary contact
        primary_contact = min(existing_contacts, key=lambda x: x.created_at)
        secondary_contacts = [c for c in existing_contacts if c.id != primary_contact.id]

        # If incoming data is new, create a secondary contact linked to the primary
        if not any(
            c.email == contact_in.email and c.phone_number == contact_in.phone_number for c in existing_contacts
        ):
            new_secondary_contact = Contact(
                email=contact_in.email,
                phone_number=contact_in.phone_number,
                linked_id=primary_contact.id,
                link_precedence="secondary"
            )
            session.add(new_secondary_contact)
            session.commit()
            session.refresh(new_secondary_contact)
            secondary_contacts.append(new_secondary_contact)

        # Prepare the response
        response = {
            "primaryContactId": primary_contact.id,
            "emails": list(set(c.email for c in [primary_contact] + secondary_contacts if c.email)),
            "phoneNumbers": list(set(c.phone_number for c in [primary_contact] + secondary_contacts if c.phone_number)),
            "secondaryContactIds": [c.id for c in secondary_contacts]
        }
        return {"contact": response}

    except Exception as e:
        session.rollback()
        logger.error(f"Error occurred: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        session.close()

