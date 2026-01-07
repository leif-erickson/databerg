# routers/users.py
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlmodel import Session
from typing import Annotated
from models.user import User, UserCreate, UserRead
from passlib.context import CryptContext
from jose import jwt
from datetime import timedelta, datetime
from auth.auth import SECRET_KEY, ALGORITHM, get_current_user
from database import get_session
from utils import error_handler

router = APIRouter()

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password)

@router.post("/", response_model=UserRead)
@error_handler(retries=3, exceptions=(Exception,))
def create_user(user: UserCreate, session: Session = Depends(get_session)):
    db_user = User(username=user.username, email=user.email, hashed_password=get_password_hash(user.password))
    session.add(db_user)
    session.commit()
    session.refresh(db_user)
    return db_user

@router.post("/token")
@error_handler(retries=3, exceptions=(Exception,))
def login_for_access_token(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]):
    session = next(get_session())
    user = session.exec(select(User).where(User.username == form_data.username)).first()
    if not user or not verify_password(form_data.password, user.hashed_password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect username or password")
    access_token_expires = timedelta(minutes=30)
    access_token = jwt.encode(
        {"sub": user.username, "exp": datetime.utcnow() + access_token_expires},
        SECRET_KEY,
        algorithm=ALGORITHM
    )
    return {"access_token": access_token, "token_type": "bearer"}

@router.get("/me", response_model=UserRead)
@error_handler(retries=3, exceptions=(Exception,))
def read_users_me(current_user: Annotated[User, Depends(get_current_user)]):
    return current_user