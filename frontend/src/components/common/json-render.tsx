"use client";

import { useState } from "react";

export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

interface JsonNodeProps {
  value: JsonValue;
  level?: number;
}

function JsonNode({ value, level = 0 }: JsonNodeProps) {
  const [isExpanded, setIsExpanded] = useState(false);
  const indent = "  ".repeat(level);

  if (value === null) {
    return <span className="text-gray-500">null</span>;
  }

  if (typeof value === "boolean") {
    return <span className="text-blue-500">{value.toString()}</span>;
  }

  if (typeof value === "number") {
    return <span className="text-green-500">{value}</span>;
  }

  if (typeof value === "string") {
    return <span className="text-orange-500">&quot;{value}&quot;</span>;
  }

  if (Array.isArray(value)) {
    if (value.length === 0) {
      return <span>[]</span>;
    }

    return (
      <div>
        <button
          onClick={() => setIsExpanded(!isExpanded)}
          className="text-gray-400 hover:text-gray-600"
        >
          {isExpanded ? "▼" : "▶"} [
        </button>
        {isExpanded && (
          <>
            {value.map((item, index) => (
              <div key={index} style={{ marginLeft: "1.5rem" }}>
                {indent}
                <JsonNode value={item} level={level + 1} />
                {index < value.length - 1 && ","}
              </div>
            ))}
            <div>{indent}]</div>
          </>
        )}
        {!isExpanded && " ... ]"}
      </div>
    );
  }

  if (typeof value === "object") {
    const entries = Object.entries(value);
    if (entries.length === 0) {
      return <span>{"{}"}</span>;
    }

    return (
      <div>
        <button
          onClick={() => setIsExpanded(!isExpanded)}
          className="text-gray-400 hover:text-gray-600"
        >
          {isExpanded ? "▼" : "▶"} {"{"}
        </button>
        {isExpanded && (
          <>
            {entries.map(([key, val], index) => (
              <div key={key} style={{ marginLeft: "1.5rem" }}>
                {indent}
                <span className="text-purple-500">&quot;{key}&quot;</span>:{" "}
                <JsonNode value={val} level={level + 1} />
                {index < entries.length - 1 && ","}
              </div>
            ))}
            <div>
              {indent}
              {"}"}
            </div>
          </>
        )}
        {!isExpanded && " ... }"}
      </div>
    );
  }

  return null;
}

export function JsonRender({
  json,
  title,
}: {
  json: JsonValue;
  title?: string;
}) {
  return (
    <div className="font-mono text-sm overflow-auto p-4 bg-primary-foreground rounded">
      {title && <h3 className="text-lg font-medium mb-3">{title}</h3>}
      <JsonNode value={json} />
    </div>
  );
}
