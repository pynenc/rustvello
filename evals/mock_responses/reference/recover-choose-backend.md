(a) One machine: `backend="sqlite"` (a file on local disk; delayed retries are durable).
(b) Three machines: `backend="postgres"`; both guarantee delayed retry in the matrix.
