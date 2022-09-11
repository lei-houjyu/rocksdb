#!/bin/bash

mv iostat.out old-iostat.out

iostat -ytN 1 > iostat.out
