import os
import logging
from typing import Optional

from fastapi import FastAPI
import aiokafka
from pydantic import BaseModel


class Item(BaseModel):
    topic: str
    key: str
    value: Optional[str] = None


app = FastAPI()

@app.on_event("startup")
async def startup_event():
    logging.error("nastartoval som")
    logging.error(os.environ['kafka_url'])



@app.get("/")
async def root():
    logging.error("OK")
    return {"message": "Hello World"}


@app.post("/publish/")
async def say_hello(name: Item):
    app.producer = aiokafka.AIOKafkaProducer(bootstrap_servers=os.environ['kafka_url'])
    await app.producer.start()
    logging.error(str({"message": "OK", "topic": name.topic, "key": name.key, "length": len(name.value)}))
    logging.error(name.value)
    await app.producer.send_and_wait(str(name.topic), str(name.value).encode(encoding="UTF8"), str(name.key).encode(encoding="UTF8"))
    await app.producer.stop()
    return {"message": "OK", "topic": name.topic, "key": name.key, "length": len(name.value)}

@app.on_event("shutdown")
async def shutdown_event():
    pass
