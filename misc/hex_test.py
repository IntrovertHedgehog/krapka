def stoh(s):
    print("0x", end="")
    for c in s:
        print(hex(ord(c))[2:], end="-")
    print()


stoh("hello!")
