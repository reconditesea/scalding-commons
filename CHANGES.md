# scalding-commons #

## 0.0.5



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
