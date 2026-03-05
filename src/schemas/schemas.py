from pydantic import BaseModel, Field
from typing import Optional, List, Any, Dict, Literal
from datetime import datetime,date

class UnifiedSearchRequest(BaseModel):
    query: str = Field(..., description="User's search query")
  
class DatabaseResponseData(BaseModel):
    response_type: Literal["database"] = "database"
    columns: List[str] = Field(default_factory=list, description="Column names")
    rows: List[Dict[str, Any]] = Field(default_factory=list, description="Result rows")
    count: int = Field(default=0, description="Total count of matching records")
    index: int = Field(description="Redis index for this result")

class MessageResponseData(BaseModel):
    response_type: Literal["message"] = "message"
    response_message: str = Field(description="Message to display to user")
     
class AISummary(BaseModel):
    text:str
    index: int
    
class AIChart(BaseModel):
    svg: str
    index: int

class ChatResponseData(BaseModel):
    response_type: Literal["ai"] = "ai"
    analysis_text:Optional[AISummary] = None
    chart:Optional[AIChart]=None
    
class HybridDatabasePart(BaseModel):
    columns: List[str] = Field(default_factory=list, description="Column names")
    rows: List[Dict[str, Any]] = Field(default_factory=list, description="Result rows")
    count: int = Field(default=0, description="Total count of matching records")
    index: int = Field(description="Redis index for this result")

class HybridAiPart(BaseModel):
    analysis_text:Optional[AISummary] = None
    chart:Optional[AIChart]=None
    
class HybridResponseData(BaseModel):
    response_type: Literal["hybrid"] = "hybrid"
    database: HybridDatabasePart
    ai: HybridAiPart

class BaseResponse(BaseModel):
    success: bool = Field(description="Indicates if request was successful")
    code: int = Field(description="HTTP status code")
    message: str = Field(description="Response message")
    errors: List[str] = Field(default_factory=list, description="List of errors if any")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Optional metadata (e.g., new_session_id)")

class DatabaseResponseWrapper(BaseResponse):
    data: DatabaseResponseData

class MessageResponseWrapper(BaseResponse):
    data: MessageResponseData

class ChatResponseWrapper(BaseResponse):
    data: ChatResponseData

class ErrorResponse(BaseResponse):
    data: None = None
    
class HybridResponseWrapper(BaseResponse):
    data: HybridResponseData


    
    
    
    
    

class ExportPdfRequest(BaseModel):
    index: int
    title: str | None = "Financial Report"
    output_dir: Optional[str] = None

class ExportWordRequest(BaseModel):
    index: int
    title: str | None = "Financial Report"

class ExportExcelRequest(BaseModel):
    index: int
    sheet_name: str | None = "Financial Data"


class ExportPngRequest(BaseModel):
    index: int
    width: int = Field(default=1920)
    height: int = Field(default=1120)
   

class HealthResponse(BaseModel):
    """Health check response"""
    status: str = Field(default="healthy")
    version: str = Field(default="5.4.0")
    gemini_available: bool
    redis_connected: bool
    database_connected: bool
    

class ComparisonRequest(BaseModel):
    AccountNumber: str 
    CurrentDate: date
   


class ComparisonFileData(BaseModel):
    response_type: Literal["comparison"] = "comparison"
    CurrentDate: date
    AccountNumber: str
    file_id: int
    file_name: str
    file_size: int
    created_at: str
    

class ComparisonResponse(BaseResponse):
    data: Optional[ComparisonFileData] = None
    
    
