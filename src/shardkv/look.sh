#!/bin/bash

ls | grep 'test-\d.log' | xargs -n 1  sed -n -e '5,8p'  | grep 'Passed' | wc -l
for i in {1..5} ; do
  ls | grep 'test-'${i}'\d.log' | xargs -n 1  sed -n -e '5,8p'  | grep 'Passed' | wc -l
done