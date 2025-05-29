import { cn } from "@/lib/utils";
import * as React from "react";
import FieldError from "../common/field-error";

export interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {
  startIcon?: React.ReactNode;
  endIcon?: React.ReactNode;
  error?: string;
}

const Input = React.forwardRef<HTMLInputElement, InputProps>(
  ({ className, type, startIcon, endIcon, error, ...props }, ref) => {
    return (
      <>
        <div className="relative w-full">
          {startIcon && (
            <div className="absolute top-1/2 left-1.5 -translate-y-1/2 transform">{startIcon}</div>
          )}
          <input
            type={type}
            className={cn(
              "border-input file:text-foreground focus-visible:ring-ring flex h-9 w-full rounded-full border bg-white px-3 py-1 text-base text-gray-700 transition-colors file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-gray-500 focus-visible:ring-1 focus-visible:outline-none disabled:cursor-not-allowed disabled:opacity-50 md:text-sm",
              startIcon ? "pl-8" : "",
              endIcon ? "pr-8" : "",
              className
            )}
            ref={ref}
            {...props}
          />
          {endIcon && (
            <div className="absolute top-1/2 right-3 -translate-y-1/2 transform">{endIcon}</div>
          )}
        </div>
        {error && <FieldError message={error} />}
      </>
    );
  }
);
Input.displayName = "Input";

export { Input };
