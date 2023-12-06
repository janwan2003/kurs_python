import math
import random
import sys


class Ułamek:
    def __init__(self, licznik, mianownik):
        assert mianownik != 0, "Mianownik nie może być zerowy"
        gcd = math.gcd(licznik, mianownik)
        self.licznik = licznik // gcd
        self.mianownik = mianownik // gcd
        if self.mianownik < 0:
            self.licznik = -self.licznik
            self.mianownik = -self.mianownik

    def __str__(self):
        return f"{self.licznik}/{self.mianownik}"

    def __repr__(self):
        return f"Ułamek({self.licznik}, {self.mianownik})"

    def __add__(self, other):
        new_licznik = self.licznik * other.mianownik + other.licznik * self.mianownik
        new_mianownik = self.mianownik * other.mianownik
        return Ułamek(new_licznik, new_mianownik)

    def __sub__(self, other):
        new_licznik = self.licznik * other.mianownik - other.licznik * self.mianownik
        new_mianownik = self.mianownik * other.mianownik
        return Ułamek(new_licznik, new_mianownik)

    def __mul__(self, other):
        new_licznik = self.licznik * other.licznik
        new_mianownik = self.mianownik * other.mianownik
        return Ułamek(new_licznik, new_mianownik)

    def __truediv__(self, other):
        new_licznik = self.licznik * other.mianownik
        new_mianownik = self.mianownik * other.licznik
        return Ułamek(new_licznik, new_mianownik)

    # Porównania
    def __eq__(self, other):
        return self.licznik == other.licznik and self.mianownik == other.mianownik

    def __lt__(self, other):
        return self.licznik * other.mianownik < other.licznik * self.mianownik

    def __le__(self, other):
        return self.licznik * other.mianownik <= other.licznik * self.mianownik

    def __gt__(self, other):
        return self.licznik * other.mianownik > other.licznik * self.mianownik

    def __ge__(self, other):
        return self.licznik * other.mianownik >= other.licznik * self.mianownik


def generuj_ulamki(n):
    return [Ułamek(random.randint(1, 10), random.randint(1, 10)) for _ in range(n)]


def wykonaj_operacje(ulamki, k):
    for _ in range(k):
        for i in range(len(ulamki)):
            ulamki[i] += ulamki[(i + 1) % len(ulamki)]


def main():
    if len(sys.argv) != 3:
        print("Użycie: python skrypt.py [n-liczba ulamkow] [k-liczba petli]")
        sys.exit(1)

    n = int(sys.argv[1])
    k = int(sys.argv[2])

    ulamki = generuj_ulamki(n)
    wykonaj_operacje(ulamki, k)


if __name__ == "__main__":
    main()
