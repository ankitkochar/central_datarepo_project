# Library
import os
import jwt
from fastapi import HTTPException, Request
from dotenv import load_dotenv
from datetime import datetime
import logging
from openai import AzureOpenAI

# Modules
from .elastic import get_user_details

# Initialization
load_dotenv
jwt_secret = os.getenv("SAARTHI_GPT_JWT_SECRET")
azure_endpoint = os.getenv("AZURE_4OMINI_ENDPOINT")
azure_key = os.getenv("AZURE_4OMINI_KEY")

client = AzureOpenAI(
        api_key=azure_key,
        azure_endpoint=azure_endpoint,
        api_version="2023-03-15-preview",
)


# Utils
async def check_token_middleware(request: Request):
    token = request.headers.get("Authorization")
    if not token:
        raise HTTPException(status_code=401, detail="Authorization header missing")

    try:
        jwt_token = token.split(" ")[1]
        decoded_jwt = jwt.decode(
            jwt_token,
            jwt_secret,
            algorithms=["HS256"],
        )
        now = datetime.now().strftime("%H:%M:%S")
        request.state.user_email = decoded_jwt['user_email']

        logging.info(f"User: {decoded_jwt['user_email']} has accessed API: {request.url.path} at {now}")
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=400, detail="Token Expired")
    except jwt.InvalidSignatureError:
        raise HTTPException(status_code=400, detail="Invalid Signature Token")
    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid Token Provided")

async def check_authorization(request: Request):
    user_id = request.state.user_email

    logging.info(f"Checking authorization for {user_id}")
    user_details = get_user_details(user_id)

    if "is_admin" in user_details:
        admin = user_details['is_admin']
        if not admin:
            logging.info(f"{user_id} Don't have Admin Access.")
            raise HTTPException(status_code=401, detail="Not Authorized!")
    else:
        logging.info(f"admin details not found for {user_id}")
        raise HTTPException(status_code=401, detail="Not Authorized!")
    
def get_response_from_gpt(prompt):
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": "You are a helpful assistant that Understands a user query and give Answer in Yes or No.",
            },
            {"role": "user", "content": prompt},
        ],
    )

    return response.choices[0].message.content