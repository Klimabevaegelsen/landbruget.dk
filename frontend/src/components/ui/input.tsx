import { cn } from "@/lib/utils";
import * as React from "react";
import FieldError from "../common/field-error";

export interface InputProps
  extends React.InputHTMLAttributes<HTMLInputElement> {
  startIcon?: React.ReactNode;
  endIcon?: React.ReactNode;
  error?: string;
}

const Input = React.forwardRef<HTMLInputElement, InputProps>(
  ({ className, type, startIcon, endIcon, error, ...props }, ref) => {
    return (
      <>
        <div className="w-full relative">
          {startIcon && (
            <div className="absolute left-1.5 top-1/2 transform -translate-y-1/2">
              {startIcon}
            </div>
          )}
          <input
            type={type}
            className={cn(
              "flex text-gray-700 h-9 w-full rounded-full border border-input bg-white px-3 py-1 text-base  transition-colors file:border-0 file:bg-transparent file:text-sm file:font-medium file:text-foreground placeholder:text-gray-500 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50 md:text-sm",
              startIcon ? "pl-8" : "",
              endIcon ? "pr-8" : "",
              className
            )}
            ref={ref}
            {...props}
          />
          {endIcon && (
            <div className="absolute right-3 top-1/2 transform -translate-y-1/2">
              {endIcon}
            </div>
          )}
        </div>
        {error && <FieldError message={error} />}
      </>
    );
  }
);
Input.displayName = "Input";

export { Input };
