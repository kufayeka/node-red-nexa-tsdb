'use strict';
// The index record: one summary of a chunk, an hour or a day. 16 float64 (128 bytes):
//   tFirst tLast vFirst vLast vMin tMin vMax tMax sum count seg off  inc integL integS  m2
// first / last / min / max carry their times. seg / off say where the chunk is (level 0).
// The last three are what lies BETWEEN the points inside the summary (the gaps between two summaries are added at query time):
//   inc     a counter's increase: the sum of the steps up, a drop counts as a restart from 0 (the new value is the step)
//   integL  the integral of the value over time, the points joined by straight lines (value x ms)
//   integS  the same with each value held until the next point (value x ms); for a bool it is the time it was true
// m2 is the sum of the squared distances of the values from their mean (variance x count): two summaries merge exactly (Chan et al.),
// with no loss when the values are large and close together (a sum of squares would lose them)
const REC = 16, RECB = REC * 8;
const F = { tFirst: 0, tLast: 1, vFirst: 2, vLast: 3, vMin: 4, tMin: 5, vMax: 6, tMax: 7, sum: 8, count: 9, seg: 10, off: 11, inc: 12, integL: 13, integS: 14, m2: 15 };
module.exports = { REC, RECB, F };
