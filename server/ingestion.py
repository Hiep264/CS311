<<<<<<< HEAD
from typing import Sequence
from qdrant_client import QdrantClient
from langchain_qdrant import QdrantVectorStore
from qdrant_client.http.models import VectorParams, Distance
from langchain_core.documents import Document
from langchain.vectorstores import VectorStore
from langchain_text_splitters import HTMLHeaderTextSplitter
from langchain_experimental.text_splitter import SemanticChunker
from langchain_community.document_loaders import CSVLoader
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from pathlib import Path
from dotenv import load_dotenv
from abc import ABC, abstractmethod
import os
import asyncio
import json


class DirectoryIngestionPipeline(ABC):
    def __init__(self, *, data_dir: str, vector_store: VectorStore):
        self.data_dir = Path(data_dir)
        self.vector_store = vector_store
        self._ingested = False

    @property
    def ingested(self):
        return self._ingested

    @abstractmethod
    async def aingest(self):
        pass


class DirectoryHtmlIngestionPipeline(DirectoryIngestionPipeline):
    def __init__(
        self,
        *,
        data_dir,
        vector_store,
        headers: list[tuple[str, str]],
    ):
        super().__init__(data_dir=data_dir, vector_store=vector_store)
        self.headers = headers

    @property
    def ingested(self):
        return self._ingested

    async def _achunk(self, file: Path):
        documents = HTMLHeaderTextSplitter(
            headers_to_split_on=self.headers,
        ).split_text_from_file(file)

        documents = await SemanticChunker(
            GoogleGenerativeAIEmbeddings(
                task_type="clustering", model="models/text-embedding-004"
            ),
            breakpoint_threshold_type="gradient",
            breakpoint_threshold_amount=90.0,
            min_chunk_size=50,
        ).atransform_documents(documents)

        return documents

    async def _aparse(self, file: Path) -> Sequence[Document]:
        return await self._achunk(file)

    async def aingest(self):
        assert (
            not self.ingested
        ), f"Data in {str(self.data_dir)} has already been ingested"

        files = [file for file in self.data_dir.glob("*.html")]
        parsed_document_sequences = await asyncio.gather(
            *(self._aparse(file) for file in files)
        )

        parsed_documents = [doc for docs in parsed_document_sequences for doc in docs]
        filtered_documents = []
        metadata = {}

        for document in parsed_documents:
            if (dieu := document.metadata.get("dieu")) is not None:
                metadata[document.metadata["tieu_de"]] = metadata.get(
                    document.metadata["tieu_de"], []
                ) + [dieu]

                filtered_documents.append(document)

        with open("metadata.json", "w") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)

        return await self.vector_store.aadd_documents(filtered_documents)


class DirectoryCsvIngestionPipeline(DirectoryIngestionPipeline):
    def __init__(self, *, data_dir, vector_store):
        super().__init__(data_dir=data_dir, vector_store=vector_store)

    async def _aparse(self, csv_file: Path):
        csv_loader = CSVLoader(csv_file)
        return csv_loader.load()

    async def aingest(self):
        assert (
            not self.ingested
        ), f"Data in {str(self.data_dir)} has already been ingested"

        files = [file for file in self.data_dir.glob("*.csv")]
        parsed_document_sequences = await asyncio.gather(
            *(self._aparse(file) for file in files)
        )

        parsed_documents = [doc for docs in parsed_document_sequences for doc in docs]
        return await self.vector_store.aadd_documents(parsed_documents)


async def aingest(
    html_pipeline: DirectoryHtmlIngestionPipeline,
    csv_pipeline: DirectoryCsvIngestionPipeline,
):
    return await asyncio.gather(html_pipeline.aingest(), csv_pipeline.aingest())


if __name__ == "__main__":
    load_dotenv()

    client = QdrantClient(
        url=os.environ["QDRANT_URL"],
        api_key=os.environ["QDRANT_API_KEY"],
    )

    if not client.collection_exists("faqs"):
        client.create_collection(
            "faqs",
            vectors_config=VectorParams(size=768, distance=Distance.COSINE),
        )

    if not client.collection_exists("legal_docs"):
        client.create_collection(
            "legal_docs",
            vectors_config=VectorParams(size=768, distance=Distance.COSINE),
        )

    faqs_vector_store = QdrantVectorStore(
        client=client,
        collection_name="faqs",
        embedding=GoogleGenerativeAIEmbeddings(
            model="models/text-embedding-004",
            task_type="retrieval_document",
        ),
    )

    legal_docs_vector_store = QdrantVectorStore(
        client=client,
        collection_name="legal_docs",
        embedding=GoogleGenerativeAIEmbeddings(
            model="models/text-embedding-004",
            task_type="retrieval_document",
        ),
    )

    html_pipeline = DirectoryHtmlIngestionPipeline(
        data_dir="data",
        vector_store=legal_docs_vector_store,
        headers=[
            ("h1", "tieu_de"),
            ("h2", "phan"),
            ("h3", "chuong"),
            ("h4", "muc"),
            ("h5", "dieu"),
        ],
    )

    csv_pipeline = DirectoryCsvIngestionPipeline(
        data_dir="data",
        vector_store=faqs_vector_store,
    )

    asyncio.run(aingest(html_pipeline, csv_pipeline))

    print("Finished ingestion!")
=======
from typing import Sequence
from qdrant_client import QdrantClient
from langchain_qdrant import QdrantVectorStore
from qdrant_client.http.models import VectorParams, Distance
from langchain_core.documents import Document
from langchain.vectorstores import VectorStore
from langchain_text_splitters import HTMLHeaderTextSplitter
from langchain_experimental.text_splitter import SemanticChunker
from langchain_community.document_loaders import CSVLoader
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from pathlib import Path
from dotenv import load_dotenv
from abc import ABC, abstractmethod
import os
import asyncio
import json


class DirectoryIngestionPipeline(ABC):
    def __init__(self, *, data_dir: str, vector_store: VectorStore):
        self.data_dir = Path(data_dir)
        self.vector_store = vector_store
        self._ingested = False

    @property
    def ingested(self):
        return self._ingested

    @abstractmethod
    async def aingest(self):
        pass


class DirectoryHtmlIngestionPipeline(DirectoryIngestionPipeline):
    def __init__(
        self,
        *,
        data_dir,
        vector_store,
        headers: list[tuple[str, str]],
    ):
        super().__init__(data_dir=data_dir, vector_store=vector_store)
        self.headers = headers

    @property
    def ingested(self):
        return self._ingested

    async def _achunk(self, file: Path):
        documents = HTMLHeaderTextSplitter(
            headers_to_split_on=self.headers,
        ).split_text_from_file(file)

        documents = await SemanticChunker(
            GoogleGenerativeAIEmbeddings(
                task_type="clustering", model="models/text-embedding-004"
            ),
            breakpoint_threshold_type="gradient",
            breakpoint_threshold_amount=90.0,
            min_chunk_size=50,
        ).atransform_documents(documents)

        return documents

    async def _aparse(self, file: Path) -> Sequence[Document]:
        return await self._achunk(file)

    async def aingest(self):
        assert (
            not self.ingested
        ), f"Data in {str(self.data_dir)} has already been ingested"

        files = [file for file in self.data_dir.glob("*.html")]
        parsed_document_sequences = await asyncio.gather(
            *(self._aparse(file) for file in files)
        )

        parsed_documents = [doc for docs in parsed_document_sequences for doc in docs]
        filtered_documents = []
        metadata = {}

        for document in parsed_documents:
            if (dieu := document.metadata.get("dieu")) is not None:
                metadata[document.metadata["tieu_de"]] = metadata.get(
                    document.metadata["tieu_de"], []
                ) + [dieu]

                filtered_documents.append(document)

        with open("metadata.json", "w") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)

        return await self.vector_store.aadd_documents(filtered_documents)


class DirectoryCsvIngestionPipeline(DirectoryIngestionPipeline):
    def __init__(self, *, data_dir, vector_store):
        super().__init__(data_dir=data_dir, vector_store=vector_store)

    async def _aparse(self, csv_file: Path):
        csv_loader = CSVLoader(csv_file)
        return csv_loader.load()

    async def aingest(self):
        assert (
            not self.ingested
        ), f"Data in {str(self.data_dir)} has already been ingested"

        files = [file for file in self.data_dir.glob("*.csv")]
        parsed_document_sequences = await asyncio.gather(
            *(self._aparse(file) for file in files)
        )

        parsed_documents = [doc for docs in parsed_document_sequences for doc in docs]
        return await self.vector_store.aadd_documents(parsed_documents)


async def aingest(
    html_pipeline: DirectoryHtmlIngestionPipeline,
    csv_pipeline: DirectoryCsvIngestionPipeline,
):
    return await asyncio.gather(html_pipeline.aingest(), csv_pipeline.aingest())


if __name__ == "__main__":
    load_dotenv()

    client = QdrantClient(
        url=os.environ["QDRANT_URL"],
        api_key=os.environ["QDRANT_API_KEY"],
    )

    if not client.collection_exists("faqs"):
        client.create_collection(
            "faqs",
            vectors_config=VectorParams(size=768, distance=Distance.COSINE),
        )

    if not client.collection_exists("legal_docs"):
        client.create_collection(
            "legal_docs",
            vectors_config=VectorParams(size=768, distance=Distance.COSINE),
        )

    faqs_vector_store = QdrantVectorStore(
        client=client,
        collection_name="faqs",
        embedding=GoogleGenerativeAIEmbeddings(
            model="models/text-embedding-004",
            task_type="retrieval_document",
        ),
    )

    legal_docs_vector_store = QdrantVectorStore(
        client=client,
        collection_name="legal_docs",
        embedding=GoogleGenerativeAIEmbeddings(
            model="models/text-embedding-004",
            task_type="retrieval_document",
        ),
    )

    html_pipeline = DirectoryHtmlIngestionPipeline(
        data_dir="data",
        vector_store=legal_docs_vector_store,
        headers=[
            ("h1", "tieu_de"),
            ("h2", "phan"),
            ("h3", "chuong"),
            ("h4", "muc"),
            ("h5", "dieu"),
        ],
    )

    csv_pipeline = DirectoryCsvIngestionPipeline(
        data_dir="data",
        vector_store=faqs_vector_store,
    )

    asyncio.run(aingest(html_pipeline, csv_pipeline))

    print("Finished ingestion!")
>>>>>>> 6c7983f64675f6b6d04f436e8ef01ac991b621ab
