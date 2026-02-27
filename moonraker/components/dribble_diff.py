#!/usr/bin/env python3
"""
Dribble Diff Tool
Compare two images (before/after nozzle heating) to detect and measure
filament dribble length. Outputs annotated image + measurements.

Usage:
    python3 dribble_diff.py before.png after.png [--crop x,y,w,h] [--px-per-mm 10.0] [--threshold 30]

Grabbing snapshots from Crowsnest:
    wget -O before.png "http://<printer-ip>:8080/?action=snapshot"
    # or for a specific cam:
    wget -O before.png "http://<printer-ip>:8080/webcam/?action=snapshot"
"""

import argparse
import os
import sys
import cv2
import numpy as np
from pathlib import Path
from dataclasses import dataclass, asdict
import json


@dataclass
class DribbleResult:
    dribble_length_px: int
    dribble_length_mm: float
    dribble_width_px: int
    dribble_area_px: int
    bounding_box: tuple  # (x, y, w, h) of the dribble region
    nozzle_tip_y: int    # detected nozzle tip y-coordinate
    image_shape: tuple


def parse_crop(crop_str: str) -> tuple:
    """Parse 'x,y,w,h' string into tuple."""
    parts = [int(x.strip()) for x in crop_str.split(",")]
    if len(parts) != 4:
        raise ValueError("Crop must be x,y,w,h")
    return tuple(parts)


def crop_image(img: np.ndarray, roi: tuple) -> np.ndarray:
    x, y, w, h = roi
    return img[y:y+h, x:x+w].copy()


def find_nozzle_tip(diff_mask: np.ndarray) -> int:
    """
    Find the topmost row of the dribble (nozzle tip).
    Scans from top to find where the diff starts.
    """
    row_sums = np.sum(diff_mask > 0, axis=1)
    active_rows = np.where(row_sums > 2)[0]
    if len(active_rows) == 0:
        return 0
    return int(active_rows[0])


def measure_dribble(
    before_path: str,
    after_path: str,
    crop_roi: tuple = None,
    threshold: int = 30,
    px_per_mm: float = None,
    min_contour_area: int = 50,
    output_dir: str = None,
) -> DribbleResult:
    """
    Compare before/after images and measure dribble.

    Args:
        before_path: path to reference image (nozzle at temp, no dribble yet)
        after_path:  path to image after dribble has formed
        crop_roi:    (x, y, w, h) to crop both images
        threshold:   pixel diff threshold (0-255)
        px_per_mm:   calibration factor, if known
        min_contour_area: ignore blobs smaller than this

    Returns:
        DribbleResult with measurements
    """
    before = cv2.imread(before_path)
    after = cv2.imread(after_path)

    if before is None:
        raise FileNotFoundError(f"Cannot read: {before_path}")
    if after is None:
        raise FileNotFoundError(f"Cannot read: {after_path}")

    if before.shape != after.shape:
        raise ValueError(
            f"Image sizes don't match: {before.shape} vs {after.shape}. "
            "Make sure both images are from the same camera position."
        )

    # Crop if requested
    if crop_roi:
        before = crop_image(before, crop_roi)
        after = crop_image(after, crop_roi)

    # Convert to grayscale
    gray_before = cv2.cvtColor(before, cv2.COLOR_BGR2GRAY)
    gray_after = cv2.cvtColor(after, cv2.COLOR_BGR2GRAY)

    # Blur to reduce noise
    gray_before = cv2.GaussianBlur(gray_before, (5, 5), 0)
    gray_after = cv2.GaussianBlur(gray_after, (5, 5), 0)

    # Absolute difference
    diff = cv2.absdiff(gray_before, gray_after)

    # Threshold
    _, mask = cv2.threshold(diff, threshold, 255, cv2.THRESH_BINARY)

    # Morphological cleanup — close small gaps, remove noise
    kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (5, 5))
    mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, kernel, iterations=2)
    mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN, kernel, iterations=1)

    # Find contours
    contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    # Filter small contours
    contours = [c for c in contours if cv2.contourArea(c) >= min_contour_area]

    if not contours:
        print("No dribble detected — images may be identical or threshold too high.")
        return DribbleResult(
            dribble_length_px=0,
            dribble_length_mm=0.0,
            dribble_width_px=0,
            dribble_area_px=0,
            bounding_box=(0, 0, 0, 0),
            nozzle_tip_y=0,
            image_shape=after.shape[:2],
        )

    # Merge all contours — the dribble may be fragmented
    all_points = np.vstack(contours)
    x, y, w, h = cv2.boundingRect(all_points)
    total_area = sum(cv2.contourArea(c) for c in contours)

    # Nozzle tip = top of the diff region
    nozzle_tip_y = find_nozzle_tip(mask)

    # Dribble length = vertical extent below the nozzle tip
    dribble_length_px = (y + h) - nozzle_tip_y

    # Convert if calibration available
    dribble_length_mm = dribble_length_px / px_per_mm if px_per_mm else 0.0

    result = DribbleResult(
        dribble_length_px=int(dribble_length_px),
        dribble_length_mm=round(dribble_length_mm, 2),
        dribble_width_px=int(w),
        dribble_area_px=int(total_area),
        bounding_box=(int(x), int(y), int(w), int(h)),
        nozzle_tip_y=int(nozzle_tip_y),
        image_shape=after.shape[:2],
    )

    # === Generate annotated output images ===
    if output_dir is not None:
        # 1. Side-by-side comparison
        comparison = np.hstack([before, after])
        cv2.putText(comparison, "BEFORE", (10, 30),
                    cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 255, 0), 2)
        cv2.putText(comparison, "AFTER", (before.shape[1] + 10, 30),
                    cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 2)
        cv2.imwrite(os.path.join(output_dir, "dribble_comparison.png"), comparison)

        # 2. Diff visualization
        diff_color = cv2.cvtColor(diff, cv2.COLOR_GRAY2BGR)
        cv2.imwrite(os.path.join(output_dir, "dribble_diff_raw.png"), diff_color)

        # 3. Annotated after image with measurements
        annotated = after.copy()

        # Draw all contours
        cv2.drawContours(annotated, contours, -1, (0, 255, 0), 2)

        # Draw bounding box
        cv2.rectangle(annotated, (x, y), (x + w, y + h), (0, 0, 255), 2)

        # Draw measurement line (nozzle tip to bottom of dribble)
        center_x = x + w // 2
        cv2.line(annotated, (center_x, nozzle_tip_y), (center_x, y + h), (255, 0, 0), 2)

        # Label
        label = f"{dribble_length_px}px"
        if px_per_mm:
            label += f" ({dribble_length_mm:.1f}mm)"
        cv2.putText(annotated, label, (x + w + 5, y + h // 2),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 0, 0), 2)

        # Nozzle tip marker
        cv2.circle(annotated, (center_x, nozzle_tip_y), 5, (0, 255, 255), -1)
        cv2.putText(annotated, "nozzle tip", (center_x + 10, nozzle_tip_y),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 255), 1)

        cv2.imwrite(os.path.join(output_dir, "dribble_annotated.png"), annotated)

        # 4. Mask output
        mask_color = cv2.cvtColor(mask, cv2.COLOR_GRAY2BGR)
        cv2.imwrite(os.path.join(output_dir, "dribble_mask.png"), mask_color)

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Measure filament dribble from before/after camera images.",
        epilog="""
Examples:
  # Basic diff:
  python3 dribble_diff.py before.png after.png

  # With crop (focus on nozzle area):
  python3 dribble_diff.py before.png after.png --crop 200,100,300,400

  # With calibration:
  python3 dribble_diff.py before.png after.png --crop 200,100,300,400 --px-per-mm 12.5

  # Grabbing images from Crowsnest:
  wget -O before.png "http://printer.local:8080/?action=snapshot"
  # <heat nozzle, wait for dribble>
  wget -O after.png "http://printer.local:8080/?action=snapshot"
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("before", help="Path to before image (no dribble)")
    parser.add_argument("after", help="Path to after image (with dribble)")
    parser.add_argument("--crop", type=str, default=None,
                        help="Crop region: x,y,w,h (e.g. 200,100,300,400)")
    parser.add_argument("--px-per-mm", type=float, default=None,
                        help="Pixels per mm calibration factor")
    parser.add_argument("--threshold", type=int, default=30,
                        help="Diff threshold 0-255 (default: 30)")
    parser.add_argument("--min-area", type=int, default=50,
                        help="Min contour area in px (default: 50)")
    parser.add_argument("--json", action="store_true",
                        help="Output result as JSON")

    args = parser.parse_args()

    crop_roi = parse_crop(args.crop) if args.crop else None

    result = measure_dribble(
        before_path=args.before,
        after_path=args.after,
        crop_roi=crop_roi,
        threshold=args.threshold,
        px_per_mm=args.px_per_mm,
        min_contour_area=args.min_area,
        output_dir=".",
    )

    if args.json:
        d = asdict(result)
        d["bounding_box"] = list(d["bounding_box"])
        d["image_shape"] = list(d["image_shape"])
        print(json.dumps(d, indent=2))
    else:
        print("\n=== Dribble Measurement Results ===")
        print(f"  Dribble length:  {result.dribble_length_px} px", end="")
        if result.dribble_length_mm:
            print(f"  ({result.dribble_length_mm} mm)")
        else:
            print("  (no px_per_mm calibration provided)")
        print(f"  Dribble width:   {result.dribble_width_px} px")
        print(f"  Dribble area:    {result.dribble_area_px} px²")
        print(f"  Bounding box:    x={result.bounding_box[0]}, y={result.bounding_box[1]}, "
              f"w={result.bounding_box[2]}, h={result.bounding_box[3]}")
        print(f"  Nozzle tip at:   y={result.nozzle_tip_y}")
        print(f"  Image size:      {result.image_shape[1]}x{result.image_shape[0]}")
        print()
        print("Output files:")
        print("  dribble_comparison.png  — side-by-side before/after")
        print("  dribble_diff_raw.png    — raw pixel difference")
        print("  dribble_mask.png        — binary detection mask")
        print("  dribble_annotated.png   — after image with measurements drawn")


if __name__ == "__main__":
    main()