import * as XLSX from 'xlsx';

const EXCEL_PATH = process.env.EXCEL_PATH!;
const SHEET_NAME = process.env.SHEET_NAME!;

const wb = XLSX.readFile(EXCEL_PATH);
const ws = wb.Sheets[SHEET_NAME];
if (!ws) throw new Error(`Sheet não existe. Sheets: ${wb.SheetNames.join(', ')}`);

const rows = XLSX.utils.sheet_to_json<Record<string, any>>(ws, { defval: '' });
console.log('Total rows:', rows.length);
console.log('Headers detectados:', Object.keys(rows[0] ?? {}));
