import pytest
from unittest.mock import mock_open, patch
from ulamek.ulamek_rozszerzony import Ułamek


@pytest.fixture(params=[(1, 2, "1/2"), (3, 4, "3/4"), (2, 3, "2/3")])
def ulamek_fixture(request):
    licznik, mianownik, expected_str = request.param
    return Ułamek(licznik, mianownik), expected_str


# Test zapisywania ułamka do pliku z wykorzystaniem fixture i parametryzacji
def test_zapisz_do_pliku(ulamek_fixture):
    ulamek, expected_content = ulamek_fixture
    nazwa_pliku = "test_ulamek.txt"

    with patch("builtins.open", mock_open()) as mocked_file:
        ulamek.zapisz_do_pliku(nazwa_pliku)
        mocked_file.assert_called_once_with(nazwa_pliku, "w")
        mocked_file().write.assert_called_once_with(expected_content)


# Test wczytywania ułamka z pliku z wykorzystaniem parametryzacji
@pytest.mark.parametrize(
    "mock_data, expected_ulamek",
    [("1/2", Ułamek(1, 2)), ("3/4", Ułamek(3, 4)), ("2/3", Ułamek(2, 3))],
)
def test_wczytaj_z_pliku(mock_data, expected_ulamek):
    nazwa_pliku = "test_ulamek.txt"

    with patch("builtins.open", mock_open(read_data=mock_data)):
        ulamek = Ułamek.wczytaj_z_pliku(nazwa_pliku)
        assert ulamek == expected_ulamek
