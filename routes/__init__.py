from .connect_to_db_routes import router as connect_to_db_routes
from .permission_routes import router as permission_routes
from .menu_routes import router as menu_routes
from .dashboard_routes import router as dashboard_routes
from .user_routes import router as user_routes
from .role_routes import router as role_routes
from .product_routes import router as product_routes
from .product_type_routes import router as product_type_routes
from .camera_routes import router as camera_routes
from .defect_type_routes import router as defect_type_routes
from .planning_routes import router as planning_routes
from .model_routes import router as model_routes
from .model_assignment_routes import router as model_assignment_routes
from .image_routes import router as image_routes
from .transaction_routes import router as transaction_routes
from .report_routes import router as report_routes
from .training_routes import router as training_routes

__all__ = [
    "connect_to_db_routes",
    "permission_routes",
    "menu_routes",
    "dashboard_routes",
    "user_routes",
    "role_routes",
    "product_routes",
    "product_type_routes",
    "camera_routes",
    "defect_type_routes",
    "planning_routes",
    "model_routes",
    "model_assignment_routes",
    "image_routes",
    "transaction_routes",
    "report_routes",
    "training_routes"
]
