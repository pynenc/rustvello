// crates/rustvello-python/src/exceptions.rs

use pyo3::create_exception;
use pyo3::exceptions::PyException;

create_exception!(rustvello.exceptions, MuseError, PyException);
create_exception!(
    rustvello.exceptions,
    MuseInitializationTimeoutError,
    MuseError
);
create_exception!(rustvello.exceptions, ConfigurationError, MuseError);
create_exception!(rustvello.exceptions, ClientError, MuseError);
create_exception!(rustvello.exceptions, RecordingError, MuseError);
create_exception!(rustvello.exceptions, ReplayingError, MuseError);
create_exception!(rustvello.exceptions, InvalidFileExtensionError, MuseError);
create_exception!(rustvello.exceptions, InvalidElementKindCodeError, MuseError);
create_exception!(
    rustvello.exceptions,
    NotAvailableRemoteElementIdError,
    MuseError
);
create_exception!(rustvello.exceptions, InvalidMetricCodeError, MuseError);
create_exception!(rustvello.exceptions, DurationConversionError, MuseError);
