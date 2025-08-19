import argparse
import shutil
import os
from ultralytics import YOLO

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--weights", type=str, required=True)
    p.add_argument("--data",    type=str, required=True)
    p.add_argument("--epochs",  type=int, default=20)
    p.add_argument("--project", type=str, default="runs/train")
    p.add_argument("--name",    type=str, required=True)
    p.add_argument("--path_output", type=str, required=True)
    p.add_argument("--idFunction",  type=int , required=True)
    p.add_argument("--subpath_output", type=str,required=True)
    p.add_argument("--type_model",  type=str, default="0")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()

    model = YOLO(args.weights)
    results = model.train(
        data=args.data,
        epochs=args.epochs,
        project=args.project,
        name=args.name,
        exist_ok=True,
    )
    idFunction = str(args.idFunction)
    source_path = os.path.join(args.project, args.name, "weights", "best.pt")
    target_dir = os.path.join("detection_app/models", args.path_output, args.subpath_output , idFunction)
    os.makedirs(target_dir, exist_ok=True)
    target_path = os.path.join(target_dir, "best.pt")

    shutil.copy2(source_path, target_path)
    print(f"Model copied to: {target_path}")

