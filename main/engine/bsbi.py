import os
import dill as pickle
import contextlib
import heapq
import math

from main.engine.index import InvertedIndexReader, InvertedIndexWriter
from main.engine.util import IdMap, sorted_merge_posts_and_tfs, process_text
from main.engine.compression import VBEPostings
from tqdm import tqdm

class BSBIIndex:
    """
    Attributes
    ----------
    term_id_map(IdMap): Untuk mapping terms ke termIDs
    doc_id_map(IdMap): Untuk mapping relative paths dari dokumen (misal,
                    /collection/0/gamma.txt) to docIDs
    data_dir(str): Path ke data
    output_dir(str): Path ke output index files
    postings_encoding: Lihat di compression.py, kandidatnya adalah StandardPostings,
                    VBEPostings, dsb.
    index_name(str): Nama dari file yang berisi inverted index
    """

    __instance = None

    @staticmethod
    def get_instance():
        if BSBIIndex.__instance == None:
            BSBIIndex(data_dir = 'collection', \
            postings_encoding = VBEPostings, \
            output_dir = 'index')
        return BSBIIndex.__instance

    def __init__(self, data_dir, output_dir, postings_encoding, index_name = "main_index"):
        if BSBIIndex.__instance != None:
            raise Exception('BSBIIndex is a Singleton class.')
        self.term_id_map = IdMap()
        self.doc_id_map = IdMap()
        self.data_dir = data_dir
        self.output_dir = output_dir
        self.index_name = index_name
        self.postings_encoding = postings_encoding

        # Untuk menyimpan nama-nama file dari semua intermediate inverted index
        self.intermediate_indices = []
        self.load()

        BSBIIndex.__instance = self

    def save(self):
        """Menyimpan doc_id_map and term_id_map ke output directory via pickle"""

        with open(os.path.join(self.output_dir, 'terms.dict'), 'wb') as f:
            pickle.dump(self.term_id_map, f)
        with open(os.path.join(self.output_dir, 'docs.dict'), 'wb') as f:
            pickle.dump(self.doc_id_map, f)

    def load(self):
        """Memuat doc_id_map and term_id_map dari output directory"""

        with open(os.path.join(self.output_dir, 'terms.dict'), 'rb') as f:
            self.term_id_map = pickle.load(f)
        with open(os.path.join(self.output_dir, 'docs.dict'), 'rb') as f:
            self.doc_id_map = pickle.load(f)

    def parse_block(self, block_dir_relative):
        """
        Lakukan parsing terhadap text file sehingga menjadi sequence of
        <termID, docID> pairs.

        Gunakan tools available untuk Stemming Bahasa Inggris

        JANGAN LUPA BUANG STOPWORDS!

        Untuk "sentence segmentation" dan "tokenization", bisa menggunakan
        regex atau boleh juga menggunakan tools lain yang berbasis machine
        learning.

        Parameters
        ----------
        block_dir_relative : str
            Relative Path ke directory yang mengandung text files untuk sebuah block.

            CATAT bahwa satu folder di collection dianggap merepresentasikan satu block.
            Konsep block di soal tugas ini berbeda dengan konsep block yang terkait
            dengan operating systems.

        Returns
        -------
        List[Tuple[Int, Int]]
            Returns all the td_pairs extracted from the block
            Mengembalikan semua pasangan <termID, docID> dari sebuah block (dalam hal
            ini sebuah sub-direktori di dalam folder collection)

        Harus menggunakan self.term_id_map dan self.doc_id_map untuk mendapatkan
        termIDs dan docIDs. Dua variable ini harus 'persist' untuk semua pemanggilan
        parse_block(...).
        """
        # TODO
        dir = f'{self.data_dir}/{block_dir_relative}'
        td_pairs = []
        for filename in os.listdir(dir):
            f = os.path.join(dir, filename)
            reader = open(f, 'rb+')

            text = reader.read().decode()
            text_tokenized = process_text(text)

            terms = [self.term_id_map[term] for term in text_tokenized]
            curr_pairs = [(term_id, self.doc_id_map[f]) for term_id in terms]
            td_pairs.extend(curr_pairs)

        return td_pairs

    def invert_write(self, td_pairs, index):
        """
        Melakukan inversion td_pairs (list of <termID, docID> pairs) dan
        menyimpan mereka ke index. Disini diterapkan konsep BSBI dimana 
        hanya di-mantain satu dictionary besar untuk keseluruhan block.
        Namun dalam teknik penyimpanannya digunakan srategi dari SPIMI
        yaitu penggunaan struktur data hashtable (dalam Python bisa
        berupa Dictionary)

        ASUMSI: td_pairs CUKUP di memori

        Di Tugas Pemrograman 1, kita hanya menambahkan term dan
        juga list of sorted Doc IDs. Sekarang di Tugas Pemrograman 2,
        kita juga perlu tambahkan list of TF.

        Parameters
        ----------
        td_pairs: List[Tuple[Int, Int]]
            List of termID-docID pairs
        index: InvertedIndexWriter
            Inverted index pada disk (file) yang terkait dengan suatu "block"
        """
        # TODO
        term_dict = {}

        for term_id, doc_id in td_pairs:
            if term_id not in term_dict:
                term_dict[term_id] = {}
                term_dict[term_id][doc_id] = 1
            else:
                if doc_id not in term_dict[term_id]:
                    term_dict[term_id][doc_id] = 1
                else:
                    term_dict[term_id][doc_id] += 1

        for term_id in sorted(term_dict.keys()):
            list_doc = []
            list_tf = []

            for doc_id in sorted(term_dict[term_id].keys()):
                list_doc.append(doc_id)
                list_tf.append(term_dict[term_id][doc_id])

            index.append(term_id, list_doc, list_tf)

    def merge(self, indices, merged_index):
        """
        Lakukan merging ke semua intermediate inverted indices menjadi
        sebuah single index.

        Ini adalah bagian yang melakukan EXTERNAL MERGE SORT

        Gunakan fungsi orted_merge_posts_and_tfs(..) di modul util

        Parameters
        ----------
        indices: List[InvertedIndexReader]
            A list of intermediate InvertedIndexReader objects, masing-masing
            merepresentasikan sebuah intermediate inveted index yang iterable
            di sebuah block.

        merged_index: InvertedIndexWriter
            Instance InvertedIndexWriter object yang merupakan hasil merging dari
            semua intermediate InvertedIndexWriter objects.
        """
        # kode berikut mengasumsikan minimal ada 1 term
        merged_iter = heapq.merge(*indices, key = lambda x: x[0])
        curr, postings, tf_list = next(merged_iter) # first item
        for t, postings_, tf_list_ in merged_iter: # from the second item
            if t == curr:
                zip_p_tf = sorted_merge_posts_and_tfs(list(zip(postings, tf_list)), \
                                                      list(zip(postings_, tf_list_)))
                postings = [doc_id for (doc_id, _) in zip_p_tf]
                tf_list = [tf for (_, tf) in zip_p_tf]
            else:
                merged_index.append(curr, postings, tf_list)
                curr, postings, tf_list = t, postings_, tf_list_
        merged_index.append(curr, postings, tf_list)
        merged_index.count_avg_doc_length()

    def retrieve_tfidf(self, query, tf_mode = 1, df_mode = 0, k = 10):
        """
        Melakukan Ranked Retrieval dengan skema TaaT (Term-at-a-Time).
        Method akan mengembalikan top-K retrieval results.

        w(t, D) = (1 + log tf(t, D))       jika tf(t, D) > 0
                = 0                        jika sebaliknya

        w(t, Q) = IDF = log (N / df(t))

        Score = untuk setiap term di query, akumulasikan w(t, Q) * w(t, D).
                (tidak perlu dinormalisasi dengan panjang dokumen)

        catatan: 
            1. informasi DF(t) ada di dictionary postings_dict pada merged index
            2. informasi TF(t, D) ada di tf_li
            3. informasi N bisa didapat dari doc_length pada merged index, len(doc_length)

        Parameters
        ----------
        query: str
            Query tokens yang dipisahkan oleh spasi

            contoh: Query "universitas indonesia depok" artinya ada
            tiga terms: universitas, indonesia, dan depok

        Result
        ------
        List[(int, str)]
            List of tuple: elemen pertama adalah score similarity, dan yang
            kedua adalah nama dokumen.
            Daftar Top-K dokumen terurut mengecil BERDASARKAN SKOR.

        JANGAN LEMPAR ERROR/EXCEPTION untuk terms yang TIDAK ADA di collection.

        """
        # TODO
        query_tokenized = process_text(query)

        with InvertedIndexReader(directory=self.output_dir, index_name=self.index_name, postings_encoding=self.postings_encoding) as reader:
            scores = {}

            for i in range(len(query_tokenized)):
                if query_tokenized[i] not in self.term_id_map:
                    continue
                
                encoded_list = reader.get_postings_list(self.term_id_map[query_tokenized[i]])
                posting_list = self.postings_encoding.decode(encoded_list[0])
                tf_list = self.postings_encoding.decode(encoded_list[1])

                wtq = 0
                if df_mode == 0:
                    wtq = math.log(len(reader.doc_length) / len(posting_list))
                elif df_mode == 1:
                    wtq = max(0, math.log((len(reader.doc_length) - len(posting_list))/ len(posting_list)))

                for j in range(len(posting_list)):
                    if posting_list[j] not in scores and tf_mode == 0:
                        scores[posting_list[j]] = wtq * tf_list[j]
                    elif posting_list[j] not in scores and tf_mode == 1:
                        scores[posting_list[j]] = wtq * (1 + math.log(tf_list[j]))
                    elif posting_list[j] in scores and tf_mode == 0:
                        scores[posting_list[j]] += wtq * tf_list[j]
                    elif posting_list[j] in scores and tf_mode == 1:
                        scores[posting_list[j]] += wtq * (1 + math.log(tf_list[j]))

        return [(score, self.doc_id_map[doc]) for doc, score in sorted(scores.items(), key=lambda item: item[1], reverse=True)][:k]
    
    def retrieve_bm25(self, query, k = 10, k1 = 1.6, b = 0.75):
        """
        Melakukan Ranked Retrieval dengan skema TaaT (Term-at-a-Time).
        Method akan mengembalikan top-K retrieval results.

        w(t, D) = (1 + log tf(t, D))       jika tf(t, D) > 0
                = 0                        jika sebaliknya

        w(t, Q) = IDF = log (N / df(t))

        Score = untuk setiap term di query, akumulasikan w(t, Q) * w(t, D).
                (tidak perlu dinormalisasi dengan panjang dokumen)

        catatan: 
            1. informasi DF(t) ada di dictionary postings_dict pada merged index
            2. informasi TF(t, D) ada di tf_li
            3. informasi N bisa didapat dari doc_length pada merged index, len(doc_length)

        Parameters
        ----------
        query: str
            Query tokens yang dipisahkan oleh spasi

            contoh: Query "universitas indonesia depok" artinya ada
            tiga terms: universitas, indonesia, dan depok

        Result
        ------
        List[(int, str)]
            List of tuple: elemen pertama adalah score similarity, dan yang
            kedua adalah nama dokumen.
            Daftar Top-K dokumen terurut mengecil BERDASARKAN SKOR.

        JANGAN LEMPAR ERROR/EXCEPTION untuk terms yang TIDAK ADA di collection.

        """
        # TODO
        query_tokenized = process_text(query)

        with InvertedIndexReader(self.index_name, self.postings_encoding, directory=self.output_dir) as reader:
            total_docs = len(reader.doc_length)
            scores = {i:0 for i in range(total_docs)}

            for term in query_tokenized:
                if term not in self.term_id_map:
                    continue
                term_id = self.term_id_map[term]
                wtq = math.log10(total_docs / reader.postings_dict[term_id][1])
                postings_list, tf_list = reader.get_postings_list(term_id)

                for i, doc_id in enumerate(self.postings_encoding.decode(postings_list)):
                    doc_weight = ((k1 + 1) * tf_list[i]) / (k1 * (1 - b + b * reader.doc_length[doc_id] / reader.avg_doc_length) + tf_list[i])
                    scores[doc_id] += doc_weight * wtq

        return [(score, self.doc_id_map[doc]) for doc, score in sorted(scores.items(), key=lambda item: item[1], reverse=True)][:k]

    def index(self):
        """
        Base indexing code
        BAGIAN UTAMA untuk melakukan Indexing dengan skema BSBI (blocked-sort
        based indexing)

        Method ini scan terhadap semua data di collection, memanggil parse_block
        untuk parsing dokumen dan memanggil invert_write yang melakukan inversion
        di setiap block dan menyimpannya ke index yang baru.
        """
        # loop untuk setiap sub-directory di dalam folder collection (setiap block)
        for block_dir_relative in tqdm(sorted(next(os.walk(self.data_dir))[1])):
            td_pairs = self.parse_block(block_dir_relative)
            index_id = 'intermediate_index_'+block_dir_relative
            self.intermediate_indices.append(index_id)
            with InvertedIndexWriter(index_id, self.postings_encoding, directory = self.output_dir) as index:
                self.invert_write(td_pairs, index)
                td_pairs = None
    
        self.save()

        with InvertedIndexWriter(self.index_name, self.postings_encoding, directory = self.output_dir) as merged_index:
            with contextlib.ExitStack() as stack:
                indices = [stack.enter_context(InvertedIndexReader(index_id, self.postings_encoding, directory=self.output_dir))
                               for index_id in self.intermediate_indices]
                self.merge(indices, merged_index)


# if __name__ == "__main__":

#     BSBI_instance = BSBIIndex(data_dir = os.path.join("main/engine", "collection"), \
#                               postings_encoding = VBEPostings, \
#                               output_dir = os.path.join("main/engine", "index"))
#     BSBI_instance.index() # memulai indexing!