from pydantic import BaseModel, Field
from typing import Optional, List, Any, Dict, Literal

class UnifiedSearchRequest(BaseModel):
    """Unified search request - router decides mode automatically"""
    query: str = Field(..., description="User's search query")
    TenantId: str = Field(..., description="Tenant identifier")
    SessionId: str = Field(..., description="Session identifier for conversation tracking")

class DatabaseSearchRequest(BaseModel):
    """Legacy - Database-only search request"""
    query: str = Field(..., description="User's search query for database")
    TenantId: str = Field(..., description="Tenant identifier")
    SessionId: str = Field(..., description="Session identifier")

class ChatSearchRequest(BaseModel):
    """Legacy - AI-only search request"""
    query: str = Field(..., description="User's query for AI analysis")
    TenantId: str = Field(..., description="Tenant identifier")
    SessionId: str = Field(..., description="Session identifier for chat memory")


class DatabaseResponseData(BaseModel):
    """Database search results"""
    response_type: Literal["database"] = "database"
    columns: List[str] = Field(default_factory=list, description="Column names")
    rows: List[Dict[str, Any]] = Field(default_factory=list, description="Result rows")
    count: int = Field(default=0, description="Total count of matching records")
    index: int = Field(description="Redis index for this result")

class MessageResponseData(BaseModel):
    """Simple message response"""
    response_type: Literal["message"] = "message"
    response_message: str = Field(description="Message to display to user")
    
    
class AISummary(BaseModel):
    text:str
    index: int
    
class AIChart(BaseModel):
    svg: str
    index: int


class ChatResponseData(BaseModel):
    """AI chat/analysis results"""
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
    """Hybrid response combining database and AI results"""
    response_type: Literal["hybrid"] = "hybrid"
    database: HybridDatabasePart
    ai: HybridAiPart

class BaseResponse(BaseModel):
    """Base response wrapper"""
    success: bool = Field(description="Indicates if request was successful")
    code: int = Field(description="HTTP status code")
    message: str = Field(description="Response message")
    errors: List[str] = Field(default_factory=list, description="List of errors if any")

class DatabaseResponseWrapper(BaseResponse):
    """Wrapper for database responses"""
    data: DatabaseResponseData

class MessageResponseWrapper(BaseResponse):
    """Wrapper for message responses"""
    data: MessageResponseData

class ChatResponseWrapper(BaseResponse):
    """Wrapper for chat/AI responses"""
    data: ChatResponseData

class ErrorResponse(BaseResponse):
    """Error response"""
    data: None = None
    
class HybridResponseWrapper(BaseResponse):
    """Wrapper for hybrid mode responses"""
    data: HybridResponseData

class ExportBaseRequest(BaseModel):
    TenantId: str
    SessionId: str
    index: int

class ExportWordRequest(ExportBaseRequest):
    title: str=Field(default="Financial Report")

class ExportPdfRequest(ExportBaseRequest):
    title: str=Field(default="Financial Report")
    
class ExportExcelRequest(ExportBaseRequest):
    sheet_name: str=Field(default="Financial Data")
    
class ExportPngRequest(ExportBaseRequest):
    width: int=Field(default=1920)
    height: int=Field(default=1120)
   

class HealthResponse(BaseModel):
    """Health check response"""
    status: str = Field(default="healthy")
    version: str = Field(default="5.4.0")
    gemini_available: bool
    redis_connected: bool
    database_connected: bool