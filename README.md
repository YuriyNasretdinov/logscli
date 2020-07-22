# logscli
CLI for logs based on top of ClickHouse

# Installation
`$ go get https://github.com/YuriyNasretdinov/logscli`

# ClickHouse table requirements
You need to have a ClickHouse table with logs that you would like to view that has the following fields:

```
time DateTime, -- event timestamp
millis UInt16, -- event timestamp milliseconds (stored separately)
```

Also, by default, the `review_body` field is assumed to have the text field as the dataset that the author used to develop this utility is based on https://s3.amazonaws.com/amazon-reviews-pds/tsv/index.txt .

# Usage

The tool supports basic grep parameters, such as `-E` (regex search), `-F` (fixed string search), `-A`, `-B`, `-C` (show matching lines in context). Also there is a `-tailf` mode.

It connects to ClickHouse server and sends queries there (`localhost:8123` is used as default).

# Examples

## Show lines containing the word 'walmart'
`$ logscli -F 'walmart' | less`

## Show the latest 10 lines that contain the word "terrible"
`$ logscli -F terrible -limit 10`

## The same as above without using -limit:
`$ logscli -F terrible | head -n 10`

## Show all lines that match /times [0-9]/ and that are written for vine and that have a high rating.
`$ logscli -E 'times [0-9]' -where="vine='Y' AND star_rating>4" | less`

## Show all lines containing "panic" and 3 context lines around them.
`$ logscli -F 'panic' -C 3 | less`

## Continiously show log messages that contain the phrase "5-star"
`$ logscli -F '5-star' -tailf`
