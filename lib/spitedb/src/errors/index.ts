export {
  SpiteDBError,
  SpiteDBNotOpenError,
  ProjectionsNotStartedError,
  ProjectionBackpressureError,
  ProjectionBackpressureTimeoutError,
} from './spitedb-error';

export {
  ProjectionError,
  ProjectionBuildError,
  ProjectionNotFoundError,
  ProjectionAlreadyRegisteredError,
  ProjectionDisabledError,
  ProjectionCoordinatorError,
  ProjectionCatchUpTimeoutError,
} from './projection-errors';

export {
  CheckpointWriteError,
  CheckpointLoadError,
  CheckpointCorruptionError,
  CheckpointVersionError,
} from './checkpoint-errors';
