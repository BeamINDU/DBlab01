from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from enum import Enum
from fastapi import UploadFile, Form, File, Body

# class OrderField(str, Enum):
#     defecttime = "defecttime"
#     prodid = "prodid"
#     prodname = "prodname"
#     cameraid = "cameraid"
#     cameraname = "cameraname"
#     prodseq = "prodseq"

# class OrderDirection(str, Enum):
#     asc = "asc"
#     desc = "desc"

class RoleSearch(BaseModel):
    rolename: Optional[str] = Field(default=None, alias="roleName")
    rolestatus: Optional[bool] = Field(default=None, alias="status")

class RoleCreate(BaseModel):
    rolename: str = Field(alias="roleName")
    roledescription: Optional[str] = Field(default=None, alias="description")
    rolestatus: Optional[bool] = Field(default=True, alias="status")
    createdby: Optional[str] = Field(default=None, alias="createdBy")

class RoleUpdate(BaseModel):
    rolename: Optional[str] = Field(default=None, alias="roleName")
    roledescription: Optional[str] = Field(default=None, alias="description")
    rolestatus: Optional[bool] = Field(default=None, alias="status")
    updatedby: Optional[str] = Field(default=None, alias="updatedBy")

class ProductSearch(BaseModel):
    prodid: Optional[str] = Field(default=None, alias="productId")
    prodname: Optional[str] = Field(default=None, alias="productName")
    prodserial: Optional[str] = Field(default=None, alias="serialNo")
    prodtypeid: Optional[str] = Field(default=None, alias="productTypeId")
    prodtype: Optional[str] = Field(default=None, alias="productTypeName")
    prodstatus: Optional[bool] = Field(default=None, alias="status")

class ProductCreate(BaseModel):
    prodid: str = Field(alias="productId")
    prodname: Optional[str] = Field(default=None, alias="productName")
    prodtypeid: str = Field(alias="productTypeId")
    prodserial: Optional[str] = Field(default=None, alias="serialNo")
    prodstatus: Optional[bool] = Field(default=True, alias="status")
    barcode: Optional[str] = Field(default=None, alias="barcode")
    packsize: Optional[int] = Field(default=0, alias="packSize")
    createdby: Optional[str] = Field(default=None, alias="createdBy")

class ProductUpdate(BaseModel):
    prodid: str = Field(alias="productId")
    prodname: Optional[str] = Field(default=None, alias="productName")
    prodtypeid: Optional[str] = Field(default=None, alias="productTypeId")
    prodtype: Optional[str] = Field(default=None, alias="productType")
    prodserial: Optional[str] = Field(default=None, alias="serialNo")
    prodstatus: Optional[bool] = Field(default=None, alias="status")
    barcode: Optional[str] = Field(default=None, alias="barcode")
    packsize: Optional[int] = Field(default=0, alias="packSize")
    updatedby: Optional[str] = Field(default=None, alias="updatedBy")

class ProdTypeSearch(BaseModel):
    prodtypeid: Optional[str] = Field(default=None, alias="productTypeId")
    prodtype: Optional[str] = Field(default=None, alias="productTypeName")
    prodstatus: Optional[bool] = Field(default=None, alias="status")

class ProdTypeCreate(BaseModel):
    prodtypeid: str = Field(alias="productTypeId")
    prodtype: str = Field(alias="productTypeName")
    proddescription: Optional[str] = Field(default=None, alias="description")
    prodstatus: Optional[bool] = Field(default=True, alias="status")
    createdby: Optional[str] = Field(default=None, alias="createdBy")

class ProdTypeUpdate(BaseModel):
    prodtypeid: str = Field(alias="productTypeId")
    prodtype: Optional[str] = Field(default=None, alias="productTypeName")
    proddescription: Optional[str] = Field(default=None, alias="description")
    prodstatus: Optional[bool] = Field(default=None, alias="status")
    updatedby: Optional[str] = Field(default=None, alias="updatedBy")

class CameraSearch(BaseModel):
    cameraid: Optional[str] = Field(default=None, alias="cameraId")
    cameraname: Optional[str] = Field(default=None, alias="cameraName")
    cameraip: Optional[str] = Field(default=None, alias="cameraIp")
    cameralocation: Optional[str] = Field(default=None, alias="location")
    camerastatus: Optional[bool] = Field(default=None, alias="status")

class CameraCreate(BaseModel):
    cameraid: str = Field(alias="cameraId")
    cameraname: str = Field(alias="cameraName")
    cameralocation: Optional[str] = Field(default=None, alias="location")
    cameraip: Optional[str] = Field(default=None, alias="cameraIp")
    camerastatus: Optional[bool] = Field(default=True, alias="status")
    createdby: Optional[str] = Field(default=None, alias="createdBy")

class CameraUpdate(BaseModel):
    cameraid: str = Field(alias="cameraId")
    cameraname: Optional[str] = Field(default=None, alias="cameraName")
    cameralocation: Optional[str] = Field(default=None, alias="location")
    cameraip: Optional[str] = Field(default=None, alias="cameraIp")
    camerastatus: Optional[bool] = Field(default=None, alias="status")
    updatedby: Optional[str] = Field(default=None, alias="updatedBy")

class UserSearch(BaseModel):
    userid: Optional[str] = Field(default=None, alias="userId")
    username: Optional[str] = Field(default=None, alias="username")
    fullname: Optional[str] = Field(default=None, alias="fullname")
    rolename: Optional[str] = Field(default=None, alias="roleName")
    userstatus: Optional[bool] = Field(default=None, alias="status")

class UserCreate(BaseModel):
    userid: str = Field(alias="userId")
    username: Optional[str] = Field(default=None)
    ufname: Optional[str] = Field(default=None, alias="firstname")
    ulname: Optional[str] = Field(default=None, alias="lastname")
    upassword: Optional[str] = None
    email: Optional[str] = None
    userstatus: Optional[bool] = Field(default=True, alias="status")
    roles: Optional[List[int]] = None
    createdby: Optional[str] = Field(default=None, alias="createdBy")

class UserUpdate(BaseModel):
    userid: str = Field(alias="userId")
    username: Optional[str] = None
    ufname: Optional[str] = Field(default=None, alias="firstname")
    ulname: Optional[str] = Field(default=None, alias="lastname")
    upassword: Optional[str] = None
    email: Optional[str] = None
    userstatus: Optional[bool] = Field(default=None, alias="status")
    roles: Optional[List[int]] = None
    updatedby: Optional[str] = Field(default=None, alias="updatedBy")

class DefectTypeSearch(BaseModel):
    defectid: Optional[str] = Field(default=None, alias="defectTypeId")
    defecttype: Optional[str] = Field(default=None, alias="defectTypeName")
    defectstatus: Optional[bool] = Field(default=None, alias="status")

class DefectTypeCreate(BaseModel):
    defectid: str = Field(alias="defectTypeId")
    defecttype: str = Field(alias="defectTypeName")
    defectdescription: Optional[str] = Field(default=None, alias="description")
    defectstatus: Optional[bool] = Field(default=True, alias="status")
    createdby: Optional[str] = Field(default=None, alias="createdBy")

class DefectTypeUpdate(BaseModel):
    defectid: str = Field(alias="defectTypeId")
    defecttype: Optional[str] = Field(default=None, alias="defectTypeName")
    defectdescription: Optional[str] = Field(default=None, alias="description")
    defectstatus: Optional[bool] = Field(default=None, alias="status")
    updatedby: Optional[str] = Field(default=None, alias="updatedBy")

class PlanningSearch(BaseModel):
    planid: Optional[str] = Field(default=None, alias="planId")
    prodid: Optional[str] = Field(default=None, alias="productId")
    prodname: Optional[str] = Field(default=None, alias="productName")
    prodlot: Optional[str] = Field(default=None, alias="lotNo")
    prodline: Optional[str] = Field(default=None, alias="lineId")
    startdatetime: Optional[datetime] = Field(default=None, alias="dateFrom")
    enddatetime: Optional[datetime] = Field(default=None, alias="dateTo")
    page: int = 1
    pageSize: int = 10
    order_by: Optional[str] = Field(default="prodlot")
    order_dir: Optional[str] = Field(default="asc")
    export_type: Optional[str] = Field(default=None, alias="exportType")

class PlanningCreate(BaseModel):
    planid: str
    prodid: str
    prodlot: str
    prodline: str
    quantity: int
    startdatetime: datetime
    enddatetime: datetime
    actualstartdatetime: Optional[datetime] = Field(default=None)
    actualenddatetime: Optional[datetime] = Field(default=None)
    createdby: Optional[str] = Field(default=None)

class PlanningUpdate(BaseModel):
    planid: str
    prodid: str
    prodlot: str
    prodline: str
    quantity: int
    startdatetime: datetime
    enddatetime: datetime
    actualstartdatetime: Optional[datetime] = Field(default=None)
    actualenddatetime: Optional[datetime] = Field(default=None)
    updatedby: Optional[str] = Field(default=None)

class PlanningStart(BaseModel):
    planid: str
    prodid: str
    prodlot: str
    prodline: str
    startby: Optional[str] = Field(default=None)

class PlanningStop(BaseModel):
    planid: str
    prodid: str
    prodlot: str
    prodline: str
    seq_no:  int
    stopby: Optional[str] = Field(default=None)

class DetectionModelSearch(BaseModel):
    modelname: Optional[str] = Field(default=None, alias="modelName")
    versionno: Optional[int] = Field(default=None, alias="version")
    function: Optional[str] = Field(default=None, alias="function")
    modelstatus: Optional[str] = Field(default=None, alias="statusId")

class DetectionModelCreate(BaseModel):
    modelname: str = Field(alias="modelName")
    modeldescription: Optional[str] = Field(default=None, alias="description")
    prodid: str = Field(alias="ProductId")
    createdby: Optional[str] = Field(default=None, alias="createdBy")

class DetectionModelDuplicate(BaseModel):
    modelversionid: int = Field(alias="modelVersionId")
    createdby: Optional[str] = Field(default=None, alias="createdBy")

class DetectionModelUpdate(BaseModel):
    modelid: int = Field(alias="modelId")
    modelname: Optional[str] = Field(default=None, alias="modelName")
    modeldescription: Optional[str] = Field(default=None, alias="description")
    updatedby: Optional[str] = Field(default=None, alias="updatedBy")

class DetectionModelUpdateStep1(BaseModel):
    modelid: int = Field(alias="modelId")
    functions: Optional[List[int]] = Field(default=None, alias="functions")
    updatedby: Optional[str] = Field(default=None, alias="updatedBy")

class DetectionModelUpdateStep2(BaseModel):
    modelid: int = Field(alias="modelId")
    prodid: Optional[str] = Field(default=None, alias="ProductId")
    modelname: Optional[str] = Field(default=None, alias="modelName")
    modeldescription: Optional[str] = Field(default=None, alias="description")
    trainpercent: Optional[int] = Field(default=None, alias="trainDataset")
    testpercent: Optional[int] = Field(default=None, alias="testDataset")
    valpercent: Optional[int] = Field(default=None, alias="validationDataset")
    epochs: Optional[int] = Field(default=None, alias="epochs")
    updatedby: Optional[str] = Field(default=None, alias="updatedBy")

class DetectionModelUpdateStep3(BaseModel):
    modelid: int = Field(alias="modelId")
    updatedby: Optional[str] = Field(default=None, alias="updatedBy")

class DetectionModelUpdateStep4(BaseModel):
    modelid: int = Field(alias="modelId")
    versionno: Optional[int] = Field(default=None, alias="version")
    updatedby: Optional[str] = Field(default=None, alias="updatedBy")

class DetectionModelImage(BaseModel):
    imageid: Optional[int] = Field(alias="imageId")
    modelversionid: int = Field(alias="modelVersionId")
    modelid: int = Field(alias="modelId")
    updatedby: str = Field(alias="updatedBy")
    annotate: Optional[dict] = {}
    filename: str
    base64: str

class TransactionSearch(BaseModel):
    prodlot: Optional[str] = Field(default=None, alias="lotNo")
    prodid: Optional[str] = Field(default=None, alias="productId")
    prodname: Optional[str] = Field(default=None, alias="productName")
    startdate: Optional[datetime] = Field(default=None, alias="dateFrom")
    enddate: Optional[datetime] = Field(default=None, alias="dateTo")
    page: int = 1
    pageSize: int = 10
    order_by: Optional[str] = Field(default="startDate")
    order_dir: Optional[str] = Field(default="desc")
    export_type: Optional[str] = Field(default=None, alias="exportType")

class ReportDefectSearch(BaseModel):
    prodlot: Optional[str] = Field(default=None, alias="lotNo")
    prodid: Optional[str] = Field(default=None, alias="productId")
    prodname: Optional[str] = Field(default=None, alias="productName")
    defectid: Optional[str] = Field(default=None, alias="defectTypeId")
    defecttype: Optional[str] = Field(default=None, alias="defectTypeName")
    page: int = 1
    pageSize: int = 10
    order_by: Optional[str] = Field(default="prodlot")
    order_dir: Optional[str] = Field(default="asc")
    export_type: Optional[str] = Field(default=None, alias="exportType")

class ReportProductSearch(BaseModel):
    startdate: Optional[datetime] = Field(default=None, alias="dateFrom")
    enddate: Optional[datetime] = Field(default=None, alias="dateTo")
    prodstatus: Optional[str] = Field(default=None, alias="status")
    prodid: Optional[str] = Field(default=None, alias="productId")
    prodname: Optional[str] = Field(default=None, alias="productName")
    defecttype: Optional[str] = Field(default=None, alias="defectTypeName")
    cameraid: Optional[str] = Field(default=None, alias="cameraId")
    cameraname: Optional[str] = Field(default=None, alias="cameraName")
    page: Optional[int] = 1
    pageSize: Optional[int] = 10
    page: int = 1
    pageSize: int = 10
    order_by: Optional[str] = Field(default="defecttime")
    order_dir: Optional[str] = Field(default="desc")
    export_type: Optional[str] = Field(default=None, alias="exportType")

class ReportProductSearchDetail(BaseModel):
    id: int
    defecttime: str = Field(alias="datetime")
    prodid: str = Field(alias="productId")
    prodseq: int = Field(alias="sequence")
    cameraid: str = Field(alias="cameraId")
    imagepath: str = Field(alias="imagePath")

class ProductDetailUpdate(BaseModel):
    defecttime: str = Field(alias="datetime")
    prodid: str = Field(alias="productId")
    prodseq: int = Field(alias="sequence")
    cameraid: str = Field(alias="cameraId")
    imagepath:str = Field(alias="imagePath")
    prodstatus: str = Field(alias="status")
    comment: Optional[str]
    actionby: str = Field(alias="actionBy")

class PermissionCreate(BaseModel):
    permissionid: int = Field(alias="permissionId")
    menuid: str = Field(alias="menuId")
    actionid: int = Field(alias="actionId")

class PermissionUpdate(BaseModel):
    menuid: Optional[str] = Field(default=None, alias="menuId")
    actionid: Optional[int] = Field(default=None, alias="actionId")

class MenuCreate(BaseModel):
    menuid: str = Field(alias="menuId")
    parentid: Optional[str] = Field(default=None, alias="parentId")
    menuname: str = Field(alias="menuName")
    icon: Optional[str] = Field(default=None, alias="icon")
    seq: int
    path: str

class MenuUpdate(BaseModel):
    parentid: Optional[str] = Field(default=None, alias="parentId")
    menuname: Optional[str] = Field(default=None, alias="menuName")
    icon: Optional[str] = Field(default=None, alias="icon")
    seq: Optional[int] = None
    path: Optional[str] = None

class DashboardFilter(BaseModel):
    start: datetime = Field(alias="startDate")
    end: datetime = Field(alias="endDate")
    productname: Optional[str] = Field(default=None, alias="productName")
    prodline: Optional[str] = Field(default=None, alias="lineNo")
    cameraid: Optional[str] = Field(default=None, alias="cameraId")

class ModelAssignmentSearch(BaseModel):
    modelname: Optional[str] = Field(default=None, alias="modelName")
    prodid: Optional[str] = Field(default=None, alias="productId")
    cameraid: Optional[str] = Field(default=None, alias="cameraId")
    appliedstatus: Optional[bool] = Field(default=None, alias="status")

class ModelAssignmentUpdate(BaseModel):
    prodid: str = Field(alias="productId")
    cameraid: str = Field(alias="cameraId")
    appliedstatus: bool = Field(alias="status")
    appliedby: str = Field(alias="appliedBy")
    modelversionid: int = Field(alias="modelVersionId")
    modelid: int = Field(alias="modelId")
    version: int = Field(alias="version")

class LabelClassUpdate(BaseModel):
    classid: Optional[int] = Field(default=0, alias="id")
    classname: str = Field(alias="name")

class ChangePasswordRequest(BaseModel):
    userid: str
    current_password: str
    new_password: str

class Config:
    orm_mode = True
    allow_population_by_field_name = True
