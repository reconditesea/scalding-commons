# scalding-commons #

## 0.1.4

* Revert compilation errors.

## 0.1.3

* Convert from Bijection to Injection for serialization.

## 0.1.1

* Added Error handling abilities to `LzoCodec[T]`

## 0.1.0

* Added `DailySuffixLzoCodec[T]`
* Added `HourlySuffixLzoCodec[T]`
* Dropped `util-core` dependency, added `bijection-core`
* VersionedKeyValSource now takes Bijections.

## 0.0.7

* Modify `VersionedKeyValSource` api to match README (change `writeIncrementalSource` to `writeIncremental`, remove old writeIncremental)

## 0.0.6

* `VersionedKeyValSource` now has separate source and sink versions, allowing the user to sink into custom versions.

## 0.0.5

* `VersionedKeyValSource` takes a reducers argument.
* added `writeIncrementalSource` to `RichPipeEx` and `TypedRichPipeEx`, opening these methods for use with extensions of `VersionedKeyValSource` (vs the default)
* `VersionedKeyValSource` now works in `HadoopTest` mode.

## 0.0.4

* Added com.twitter.scalding.commons.RangedArgs, for accepting pairs of arguments defining some range (dates, for example)
* com.twitter.scalding.commons.extensions.Checkpoint
* LzoCodec trait
* LzoCodecSource object

## 0.0.3

Added a large collection of Daily and Hourly LZO, thrift and protobuf based sources.

### Lzo Traits

* LzoProtobuf
* LzoThrift
* LzoText
* LzoTsv
* LzoTypedTsv

### Daily Sources

* DailySuffixLzoProtobuf
* DailySuffixLzoThrift
* TimePathedLongThriftSequenceFile
* MostRecentGoodLongThriftSequenceFile
* DailySuffixLongThriftSequenceFile
* DailySuffixLzoTsv

### Hourly Sources

* HourlySuffixLzoTsv
* HourlySuffixLzoThrift
* HourlySuffixLzoProtobuf
* HourlySuffixLzoText

### Fixed Path Sources

* FixedPathLzoThrift
* FixedPathLzoProtobuf

### Misc

* LongThriftTransformer
* TsvWithHeader

## 0.0.2

* CodecSource[T]
* PailSource[T]

## 0.0.1

* Implemented VersionedKeyValSource.
