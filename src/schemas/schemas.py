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


class ModelConfigUpsertRequest(BaseModel):
    Purpose: str = Field(..., max_length=100)
    Provider: str = Field(..., max_length=100)
    ModelName: str = Field(..., max_length=100)
    CredentialsRef: str = Field(default="")
    SecretValue: Optional[str] = None
    Config: Dict[str, Any] = Field(default_factory=dict)


class ModelConfigData(BaseModel):
    Id: int
    TenantId: str
    Purpose: str
    Provider: str
    ModelName: str
    CredentialsRef: str
    Config: Dict[str, Any] = Field(default_factory=dict)
    CreatedAt: Optional[str] = None
    UpdatedAt: Optional[str] = None


class ModelConfigResponse(BaseResponse):
    data: Optional[ModelConfigData] = None


class ModelConfigListResponse(BaseResponse):
    data: List[ModelConfigData] = Field(default_factory=list)


class ModelConfigDeleteData(BaseModel):
    Id: int


class ModelConfigDeleteResponse(BaseResponse):
    data: Optional[ModelConfigDeleteData] = None


class ModelConfigOptionsData(BaseModel):
    purposes: List[str] = Field(default_factory=list)
    providers: List[str] = Field(default_factory=list)
    models_by_provider: Dict[str, List[str]] = Field(default_factory=dict)


class ModelConfigOptionsResponse(BaseResponse):
    data: ModelConfigOptionsData


class VoiceTranscriptionData(BaseModel):
    transcript: str
    provider: str
    model_name: str


class VoiceTranscriptionResponse(BaseResponse):
    data: Optional[VoiceTranscriptionData] = None


class TokenConsumptionRecord(BaseModel):
    UserId: str
    FromDate: str
    ToDate: str
    Provider: str
    InputTokens: int
    OutputTokens: int
    TotalTokens: int


class TokenConsumptionListResponse(BaseResponse):
    data: List[TokenConsumptionRecord] = Field(default_factory=list)


class TokenConsumptionOptionsData(BaseModel):
    UserIds: List[str] = Field(default_factory=list)
    Providers: List[str] = Field(default_factory=list)


class TokenConsumptionOptionsResponse(BaseResponse):
    data: TokenConsumptionOptionsData
