declare module 'react-scrollama' {
  import { ComponentType, ReactNode } from 'react';

  interface ScrollamaProps {
    offset?: number;
    threshold?: number;
    onStepEnter?: (response: StepResponse) => void;
    onStepExit?: (response: StepResponse) => void;
    onStepProgress?: (response: StepProgressResponse) => void;
    children: ReactNode;
  }

  interface StepResponse {
    element: HTMLElement;
    data: unknown;
    direction: 'up' | 'down';
    entry: IntersectionObserverEntry;
  }

  interface StepProgressResponse extends StepResponse {
    progress: number;
  }

  interface StepProps {
    data?: unknown;
    children: ReactNode;
  }

  export const Scrollama: ComponentType<ScrollamaProps>;
  export const Step: ComponentType<StepProps>;
}
