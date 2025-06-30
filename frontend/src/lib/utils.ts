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

export function slugify(text: string) {
  return text.toLowerCase().replace(/ /g, "-");
}

export function scrollToElement(elementId: string, offset: number = 0) {
  const element = document.getElementById(elementId);
  if (!element) return;

  const elementPosition = element.getBoundingClientRect().top;
  const offsetPosition = elementPosition + window.pageYOffset - offset;

  window.scrollTo({
    top: offsetPosition,
    behavior: "smooth",
  });
}
