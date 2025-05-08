interface FieldErrorType {
  message: string;
}

const FieldError = ({ message }: FieldErrorType) => {
  return <div className={"text-xs text-highlight"}>{message}</div>;
};

export default FieldError;
