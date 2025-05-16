import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export const VizColors = [
  "#4F5D75",
  "#C67750",
  "#467968",
  "#775120",
  "#7F2E39",
  "#2D673D",
  "#503955",
  "#5F318B",
];

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}
