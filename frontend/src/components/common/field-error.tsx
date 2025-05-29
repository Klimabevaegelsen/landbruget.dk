interface FieldErrorType {
  message: string;
}

const FieldError = ({ message }: FieldErrorType) => {
  return <div className={"text-highlight text-xs"}>{message}</div>;
};

export default FieldError;
