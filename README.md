# Rate Limiting Library for the brave

slapdash is as the name implies approaches rate limiting in a careless, hasty, or haphazard manner.
instead of having to track agents precisely it uses bloomfilters to rapidly identify the heaviest
usage agents quickly and restricts them.

by taking this approach slashdash in its default settings protect an endpoint from a large number of attackers
while using O(1) memory, minimal allocations, sub millisecond response times, and a entirely gracefuly failure
scenario.
