from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
import re
import math

class IdMap:
    """
    Ingat kembali di kuliah, bahwa secara praktis, sebuah dokumen dan
    sebuah term akan direpresentasikan sebagai sebuah integer. Oleh
    karena itu, kita perlu maintain mapping antara string term (atau
    dokumen) ke integer yang bersesuaian, dan sebaliknya. Kelas IdMap ini
    akan melakukan hal tersebut.
    """

    def __init__(self):
        """
        Mapping dari string (term atau nama dokumen) ke id disimpan dalam
        python's dictionary; cukup efisien. Mapping sebaliknya disimpan dalam
        python's list.

        contoh:
            str_to_id["halo"] ---> 8
            str_to_id["/collection/dir0/gamma.txt"] ---> 54

            id_to_str[8] ---> "halo"
            id_to_str[54] ---> "/collection/dir0/gamma.txt"
        """
        self.str_to_id = {}
        self.id_to_str = []

    def __len__(self):
        """Mengembalikan banyaknya term (atau dokumen) yang disimpan di IdMap."""
        return len(self.id_to_str)

    def __get_str(self, i):
        """Mengembalikan string yang terasosiasi dengan index i."""
        # TODO
        return self.id_to_str[i]

    def __get_id(self, s):
        """
        Mengembalikan integer id i yang berkorespondensi dengan sebuah string s.
        Jika s tidak ada pada IdMap, lalu assign sebuah integer id baru dan kembalikan
        integer id baru tersebut.
        """
        # TODO
        if s in self.str_to_id:
            return self.str_to_id[s]
        else:
            self.id_to_str.append(s)
            self.str_to_id[s] = len(self.id_to_str) - 1
            return self.str_to_id[s]

    def __getitem__(self, key):
        """
        __getitem__(...) adalah special method di Python, yang mengizinkan sebuah
        collection class (seperti IdMap ini) mempunyai mekanisme akses atau
        modifikasi elemen dengan syntax [..] seperti pada list dan dictionary di Python.

        Silakan search informasi ini di Web search engine favorit Anda. Saya mendapatkan
        link berikut:

        https://stackoverflow.com/questions/43627405/understanding-getitem-method

        Jika key adalah integer, gunakan __get_str;
        jika key adalah string, gunakan __get_id
        """
        if type(key) is int:
            return self.__get_str(key)
        elif type(key) is str:
            return self.__get_id(key)
        else:
            raise TypeError

def sorted_merge_posts_and_tfs(posts_tfs1, posts_tfs2):
    """
    Menggabung (merge) dua lists of tuples (doc id, tf) dan mengembalikan
    hasil penggabungan keduanya (TF perlu diakumulasikan untuk semua tuple
    dengn doc id yang sama), dengan aturan berikut:

    contoh: posts_tfs1 = [(1, 34), (3, 2), (4, 23)]
            posts_tfs2 = [(1, 11), (2, 4), (4, 3 ), (6, 13)]

            return   [(1, 34+11), (2, 4), (3, 2), (4, 23+3), (6, 13)]
                   = [(1, 45), (2, 4), (3, 2), (4, 26), (6, 13)]

    Parameters
    ----------
    list1: List[(Comparable, int)]
    list2: List[(Comparable, int]
        Dua buah sorted list of tuples yang akan di-merge.

    Returns
    -------
    List[(Comparablem, int)]
        Penggabungan yang sudah terurut
    """
    # TODO
    result = []

    i = 0
    j = 0

    while i < len(posts_tfs1) and j < len(posts_tfs2):
        if posts_tfs1[i][0] == posts_tfs2[j][0]:
            result.append((posts_tfs1[i][0], posts_tfs1[i][1] + posts_tfs2[j][1]))
            i += 1 ; j += 1
        elif posts_tfs1[i][0] < posts_tfs2[j][0]:
            result.append(posts_tfs1[i])
            i += 1
        else:
            result.append(posts_tfs2[j])
            j += 1
    
    if len(posts_tfs1) > len(posts_tfs2):
        result += posts_tfs1[i:]
    elif len(posts_tfs1) < len(posts_tfs2):
        result += posts_tfs2[j:]

    return result

def test(output, expected):
    """ simple function for testing """
    return "PASSED" if output == expected else "FAILED"

def process_text(text):
    text = text.lower().strip() # Mengubah uppercase menjadi lowercase dan melakukan trimming pada teks
    text = re.sub("\d", "", text) # Menghilangkan angka
    text = re.sub("\s+", " ", text) # Menghilangkan spasi berlebih
    text = re.sub("[^\w\s]", "", text) # Menghilangkan tanda baca ############

    stop_words = set(stopwords.words('english'))
    stemmer = PorterStemmer()
    text_tokenized = word_tokenize(text)
    text_tokenized = [stemmer.stem(w) for w in text_tokenized if w not in stop_words]

    return text_tokenized
