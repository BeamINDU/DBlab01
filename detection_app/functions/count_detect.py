from typing import Optional, Union, List

def object_counting(
    frame: str,
    class_label: Optional[str] = None,
    box=None,
    expected: Optional[Union[int, str, List[Union[int, str]]]] = None,
    model=None
) -> dict:
    """
    Count how many polygons/boxes are in the input.
    Returns count for this frame only (no memory).
    Accepts box as either a single polygon (list of points) or a list of polygons.
    class_label is optional.
    """
    if expected is None:
        expected_value = 40
        expected_list = [expected_value]
    elif isinstance(expected, (int, float)):
        expected_list = [expected]
    elif isinstance(expected, str):
        try:
            expected_list = [int(expected)]
        except Exception:
            expected_list = [expected]  # fallback if not a number
    elif isinstance(expected, list):
        # Try to coerce list elements to int
        new_list = []
        for e in expected:
            try:
                new_list.append(int(e))
            except Exception:
                new_list.append(e)
        expected_list = new_list
    else:
        expected_list = [expected]

    # --- Detection logic ---
    if box is None:
        count = 0
    elif isinstance(box, list):
        if len(box) == 0:
            count = 0
        elif isinstance(box[0], (list, tuple)):
            if len(box[0]) == 2 and all(isinstance(coord, (float, int)) for coord in box[0]):
                count = 1
            else:
                count = len(box)
        else:
            count = 0
    else:
        count = 0

    result = {
        "predictedResult": count,
        "status": "OK" if count in expected_list else "NG",
        "expected": expected,
    }
    return result

