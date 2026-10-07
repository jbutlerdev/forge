/**
 * Minimal standard 5-field cron (minute hour day-of-month month day-of-week).
 *
 * Supports *, numbers, lists (1,2,3), ranges (1-5), and steps (for
 * example 0-10/2). @hourly/@daily/@weekly/@monthly/@yearly/@annually are
 * the only named forms accepted. This is deliberately small:
 * the durable-timer surface (H2.3) may replace it with a richer scheduler.
 */

export type CronSpec = {
	readonly minute: readonly number[];
	readonly hour: readonly number[];
	readonly dom: readonly number[];
	readonly month: readonly number[];
	readonly dow: readonly number[];
	/** True when the raw field was `*` (or an equivalent full list). Standard
	 * cron OR-semantics: when both day-of-month and day-of-week are restricted,
	 * a day matches when EITHER matches; when one is unrestricted, the other
	 * alone decides. */
	readonly domUnrestricted: boolean;
	readonly dowUnrestricted: boolean;
};

const ALIASES: Record<string, string> = {
	"@hourly": "0 * * * *",
	"@daily": "0 0 * * *",
	"@weekly": "0 0 * * 0",
	"@monthly": "0 0 1 * *",
	"@yearly": "0 0 1 1 *",
	"@annually": "0 0 1 1 *",
};

export function parseCron(expr: string): CronSpec {
	let raw = expr.trim();
	if (raw.startsWith("@")) {
		const alias = ALIASES[raw];
		if (alias === undefined) throw new Error(`Unsupported cron form: ${raw}`);
		raw = alias;
	}
	const fields = raw.split(/\s+/);
	if (fields.length !== 5) throw new Error(`Cron expression must have 5 fields: ${raw}`);

	const dom = parseField(fields[2]!, 1, 31);
	const dow = parseField(fields[4]!, 0, 6);
	return {
		minute: parseField(fields[0]!, 0, 59).values,
		hour: parseField(fields[1]!, 0, 23).values,
		dom: dom.values,
		dow: dow.values,
		month: parseField(fields[3]!, 1, 12).values,
		domUnrestricted: dom.unrestricted,
		dowUnrestricted: dow.unrestricted,
	};
}

function parseField(field: string, min: number, max: number): { values: number[]; unrestricted: boolean } {
	const values = new Set<number>();
	for (const part of field.split(",")) {
		const [rangePart, stepText] = part.includes("/") ? part.split("/") : [part];
		const step = stepText === undefined ? 1 : Number(stepText);
		if (!Number.isInteger(step) || step < 1) throw new Error(`Invalid cron step in ${part}`);
		let lo: number;
		let hi: number;
		if (rangePart === "*") {
			lo = min;
			hi = max;
		} else if (rangePart.includes("-")) {
			const [a, b] = rangePart.split("-");
			lo = Number(a);
			hi = Number(b);
			if (!Number.isInteger(lo) || !Number.isInteger(hi) || lo > hi) throw new Error(`Invalid cron range ${part}`);
		} else {
			lo = Number(rangePart);
			hi = rangePart === "*" ? max : lo;
		}
		if (lo < min || hi > max || lo > hi) throw new Error(`Cron value out of range: ${part}`);
		for (let v = lo; v <= hi; v += step) values.add(v);
	}
	const unrestricted = values.size === max - min + 1;
	return { values: [...values], unrestricted };
}

/**
 * The next fire time strictly after `fromMs`. Returns `undefined` when no
 * matching time exists within the next 4 years (an impossible combination).
 */
export function nextCronTime(spec: CronSpec, fromMs: number): number | undefined {
	const start = Math.floor(fromMs / 60_000) + 1; // whole minutes, strictly after
	const limit = start + 366 * 24 * 60 * 4;
	for (let m = start; m < limit; m++) {
		const date = new Date(m * 60_000);
		if (!spec.month.includes(date.getUTCMonth() + 1)) continue;
		const day = date.getUTCDate();
		const dow = date.getUTCDay();
		const domOk = spec.dom.includes(day);
		const dowOk = spec.dow.includes(dow);
		const dayOk = spec.domUnrestricted && spec.dowUnrestricted
			? true
			: spec.domUnrestricted
				? dowOk
				: spec.dowUnrestricted
					? domOk
					: domOk || dowOk;
		if (!dayOk) continue;
		if (!spec.hour.includes(date.getUTCHours())) continue;
		if (!spec.minute.includes(date.getUTCMinutes())) continue;
		return m * 60_000;
	}
	return undefined;
}
