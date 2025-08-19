from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from routes import connect_to_db_routes, permission_routes, menu_routes, dashboard_routes, user_routes, role_routes, product_routes, product_type_routes, camera_routes, defect_type_routes, planning_routes, model_routes, model_assignment_routes, image_routes, transaction_routes, report_routes, training_routes

app = FastAPI(
    title="PI Backend API",
    description="Backend service for managing users, roles, products, planning, inspection, and reporting.",
    version="1.0.0",
  )

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # or frontend IP 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/dataset", StaticFiles(directory="dataset"), name="dataset") 
app.mount("/result", StaticFiles(directory="product_defect_result"), name="result")

app.include_router(connect_to_db_routes)
app.include_router(permission_routes)
app.include_router(menu_routes)
app.include_router(dashboard_routes)
app.include_router(user_routes)
app.include_router(role_routes)
app.include_router(product_routes)
app.include_router(product_type_routes)
app.include_router(camera_routes)
app.include_router(defect_type_routes)
app.include_router(planning_routes)
app.include_router(model_routes)
app.include_router(model_assignment_routes)
app.include_router(image_routes)
app.include_router(transaction_routes)
app.include_router(report_routes)
app.include_router(training_routes)


# -------------------- Run Server --------------------
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
