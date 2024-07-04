#!/usr/bin/env python3

import json
import sys 

def main():
    print("Parsing input", file=sys.stderr)
    input_data = sys.stdin.read()
    parsed = json.loads(input_data)
    # TODO: Do something
    

if __name__ == "__main__":
    main()

