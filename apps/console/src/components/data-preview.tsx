import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Empty, JsonBlock } from "./common";
import type { Json } from "@/lib/types";

function display(value: Json | undefined) {
  return value === undefined || value === null
    ? "—"
    : typeof value === "object"
      ? JSON.stringify(value)
      : String(value);
}

export function DataPreview({ value }: { value: Json }) {
  if (
    Array.isArray(value) &&
    value.length > 0 &&
    value.every(
      (row) => row !== null && typeof row === "object" && !Array.isArray(row),
    )
  ) {
    const rows = value as Record<string, Json>[];
    const columns = Array.from(
      new Set(rows.flatMap((row) => Object.keys(row))),
    ).slice(0, 8);
    return (
      <>
        <div className="overflow-x-auto rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                {columns.map((column) => (
                  <TableHead key={column}>{column}</TableHead>
                ))}
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.map((row, index) => (
                <TableRow key={index}>
                  {columns.map((column) => (
                    <TableCell key={column} title={display(row[column])}>
                      {display(row[column])}
                    </TableCell>
                  ))}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
        <p className="mt-2 text-xs text-muted-foreground">
          Committed snapshot preview · up to 100 rows and 8 columns
        </p>
      </>
    );
  }
  if (Array.isArray(value) && value.length === 0)
    return (
      <Empty title="Empty dataset">
        This materialization committed zero rows.
      </Empty>
    );
  return <JsonBlock value={value} />;
}
