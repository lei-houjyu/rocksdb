#!/bin/bash

mv iostat.out old-iostat.out

iostat -yt 1 > iostat.out
