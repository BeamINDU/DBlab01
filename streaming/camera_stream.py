import cv2

# read from forwarded stream (via FFMPEG)
cap = cv2.VideoCapture("udp://0.0.0.0:1234")

def gen_frames():
    if not cap.isOpened():
        print("Could not open stream")
        return
    while True:
        success, frame = cap.read()
        if not success:
            print("Frame read failed")
            break
        _, buffer = cv2.imencode('.jpg', frame)
        frame = buffer.tobytes()
        yield (b'--frame\r\n'
               b'Content-Type: image/jpeg\r\n\r\n' + frame + b'\r\n')

