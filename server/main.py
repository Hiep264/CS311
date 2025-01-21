from pydantic import BaseModel
from qdrant_client import QdrantClient
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_qdrant import QdrantVectorStore
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os

from rag.self import SelfRagGraphBuilder
from rag.vanilla import VanillaRagGraphBuilder

load_dotenv()

client = QdrantClient(
    url=os.environ["QDRANT_URL"],
    api_key=os.environ["QDRANT_API_KEY"],
)

faqs_vector_store = QdrantVectorStore(
    client=client,
    collection_name="faqs",
    embedding=GoogleGenerativeAIEmbeddings(
        model="models/text-embedding-004",
        task_type="retrieval_query",
    ),
)

legal_docs_vector_store = QdrantVectorStore(
    client=client,
    collection_name="legal_docs",
    embedding=GoogleGenerativeAIEmbeddings(
        model="models/text-embedding-004",
        task_type="retrieval_query",
    ),
)

self_rag_graph = SelfRagGraphBuilder(
    faqs_vector_store=faqs_vector_store,
    legal_docs_vector_store=legal_docs_vector_store,
).build()

self_rag_graph.get_graph(xray=True).draw_png(output_file_path="self_rag_graph.png")

vanilla_rag_graph = VanillaRagGraphBuilder(
    faqs_vector_store=faqs_vector_store,
    legal_docs_vector_store=legal_docs_vector_store,
).build()

vanilla_rag_graph.get_graph(xray=True).draw_png(
    output_file_path="vanilla_rag_graph.png"
)

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class SelfRagPayload(BaseModel):
    question: str
    max_retrieve_try: int = 2
    max_generation_try: int = 2


class SelfRagResponse(BaseModel):
    answer: str


@app.post("/api/v1/chat/self_rag", response_model=SelfRagResponse)
async def self_rag_answer(payload: SelfRagPayload):
    response = await self_rag_graph.ainvoke(
        {
            "question": payload.question,
            "max_retrieve_try": payload.max_retrieve_try,
            "current_retrieve_try": 0,
            "max_generation_try": payload.max_generation_try,
            "current_generation_try": 0,
        }
    )
    answer: str = response.get("answer", "Xin lỗi, tôi không thể trả lời câu hỏi này.")

    return SelfRagResponse(answer=answer)


class VanillaRagPayload(BaseModel):
    question: str


class VanillaRagResponse(BaseModel):
    answer: str


@app.post("/api/v1/chat/vanilla_rag", response_model=VanillaRagResponse)
async def vanilla_rag_answer(payload: VanillaRagPayload):
    response = await vanilla_rag_graph.ainvoke({"question": payload.question})
    answer: str = response.get("answer", "Xin lỗi, tôi không thể trả lời câu hỏi này.")

    return VanillaRagResponse(answer=answer)
