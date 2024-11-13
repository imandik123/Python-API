from fastapi import FastAPI,HTTPException,Depends
from pydantic import BaseModel,Field,EmailStr,field_validator #error handling and validation
from datetime import datetime
from typing import List,Dict,AsyncGenerator
import re
import asyncio

app = FastAPI()

def authorize(api_key: str):
    if api_key!= "expected api key":
        raise HTTPException(status_code=403, detail="Unauthorized")
    
class Item(BaseModel):
    item_id: str = Field(..., min_length=1) 
    quantity: int = Field(..., gt=0)
    price: float = Field(..., gt=0) 

class DataModel(BaseModel):
      user_id: str = Field(..., min_length=1)
      email: EmailStr
      timestamp: datetime
      items: List[Item] = Field(...,min_items=1)

@field_validator("timestamp") 
def validate_timestamp(value):
     if not isinstance(value, datetime):
          raise ValueError("Timestamp must be in ISO 8601 Format")
     return value

#endpoint to receive and validate data
@app.post("/validate_data")
async def validate_data(data: DataModel, api_key:str = Depends(AsyncGenerator[Dict, None])):
     #if data validation is successful, it will be parsed into Data Model
     return {"message": "Data validated and accepted", "data": data.model_dump()} 

async def process_large_dataset() -> AsyncGenerator[Dict, None]: 
     for i in range(1000000): #assuming we are running a large dataset
          yield{"record_id": i, "data": f"data_value_{i}"}

@app.get("/process data")
async def process_data(api_key:str = Depends(authorize)):
     results = []
     async for record in process_large_dataset():
          #process each record as it comes
          results.append(record)    
          if len(results) >= 1000: 
               results = []  #clearing results after prrocessing a batch

     return {"message": "Data Processed successfully"}         

#large dataset using threading
async def process_record(record): 
     await asyncio.sleep(0)
     return {"record_id": record['id'], "data": f"processed_{record{"data"}}"}}
async def process_data_in_parallel(records):
     tasks= [process_record]  