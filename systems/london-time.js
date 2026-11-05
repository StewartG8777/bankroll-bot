'use strict';
function londonParts(ms) {
  const f = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/London',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23',
  });
  const p = Object.fromEntries(f.formatToParts(new Date(ms)).map((x) => [x.type, x.value]));
  return { y: +p.year, m: +p.month, d: +p.day, h: +p.hour, mi: +p.minute, s: +p.second };
}
function londonOffset(ms) {
  const p = londonParts(ms);
  return Date.UTC(p.y, p.m - 1, p.d, p.h, p.mi, p.s) - Math.floor(ms / 1000) * 1000;
}
function londonMidnightUnix(year, monthIndex, day) {
  const guess = Date.UTC(year, monthIndex, day);
  return Math.floor((guess - londonOffset(guess)) / 1000);
}
function periodBounds(period) {
  if (period === 'all') return { since: null, until: null };
  const p = londonParts(Date.now());
  const today = Date.UTC(p.y, p.m - 1, p.d);
  const dow = new Date(today).getUTCDay();
  const monday = today - ((dow + 6) % 7) * 86400000;
  let start;
  let end;
  if (period === 'today') {
    start = today;
    end = today + 86400000;
  } else if (period === 'week') {
    start = monday;
    end = monday + 7 * 86400000;
  } else if (period === 'last_week') {
    start = monday - 7 * 86400000;
    end = monday;
  } else if (period === 'month') {
    start = Date.UTC(p.y, p.m - 1, 1);
    end = Date.UTC(p.y, p.m, 1);
  } else if (period === 'last_month') {
    start = Date.UTC(p.y, p.m - 2, 1);
    end = Date.UTC(p.y, p.m - 1, 1);
  } else {
    throw new Error('Unknown totals period.');
  }
  const startDate = new Date(start);
  const endDate = new Date(end);
  return {
    since: londonMidnightUnix(startDate.getUTCFullYear(), startDate.getUTCMonth(), startDate.getUTCDate()),
    until: londonMidnightUnix(endDate.getUTCFullYear(), endDate.getUTCMonth(), endDate.getUTCDate()),
  };
}
module.exports = { londonParts, londonOffset, londonMidnightUnix, periodBounds };
