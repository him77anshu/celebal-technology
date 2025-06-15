#Kevin and Stuart want to play the 'The Minion Game'.

def minion_game(string):
    vowels = 'AEIOU'
    k_s = 0
    s_s = 0
    n = len(string) 
    for i in range(n):
        if string[i] in vowels:
            k_s += n - i
        else:
            s_s += n - i
    if k_s > s_s:
        print("Kevin", k_s)
    elif s_s > k_s:
        print("Stuart", s_s)
    else:
        print("Draw")
if __name__ == '__main__':
    s = input()
    minion_game(s)