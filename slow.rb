#!/usr/bin/env ruby
STDOUT.sync = 1
loop do
	print STDIN.read(1) || exit
	STDOUT.flush
	sleep (ARGV.first || "0.01").to_f
end
