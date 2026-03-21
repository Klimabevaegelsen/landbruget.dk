interface FieldErrorType {
  message: string;
}

export const FieldError = ({ message }: FieldErrorType) => {
  return <div className={'text-highlight text-xs'}>{message}</div>;
};
