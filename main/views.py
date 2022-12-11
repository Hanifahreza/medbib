import os
import re
import time

from django.core.paginator import EmptyPage, PageNotAnInteger, Paginator
from django.http import HttpResponseNotFound, HttpResponseBadRequest
from django.shortcuts import render

from main.engine.util import process_text
from main.engine.bsbi import BSBIIndex
from main.engine.compression import VBEPostings


def index(request):
    return render(request, "index.html")


def search(request):
    print(request.GET)
    if "q" not in request.GET:
        return HttpResponseBadRequest("Search query required")

    start_time = time.time()

    query = request.GET["q"]
    docs = get_serp(query)

    page_number = request.GET.get("page")
    page = paginate(docs, page_number)

    context = {
        "query": query,
        "exe_time": round(time.time() - start_time, 2),
        "page": page,
    }
    return render(request, "results.html", context=context)


def paginate(objects, page_number, doc_per_page=10):
    paginator = Paginator(objects, doc_per_page)

    try:
        page = paginator.page(page_number)
    except PageNotAnInteger:
        page = paginator.page(1)
    except EmptyPage:
        page = paginator.page(paginator.num_pages)
    return page


def get_serp(query):
    try:
        BSBI_instance = BSBIIndex(data_dir = os.path.join("main/engine", "collection"), \
                                postings_encoding = VBEPostings, \
                                output_dir = os.path.join("main/engine", "index"))
    except:
        BSBI_instance = BSBIIndex.get_instance()

    docs = []
    for (_, doc_path) in BSBI_instance.retrieve_bm25(query, k=100):
        doc_id = doc_path.split('/')[-1][:-4]
        f = open(doc_path)
        clean_query = process_text(query)

        col_id, doc_id_disp = doc_path[12:].split('/')[1:]
        title, content = get_title_content(f, col_id, doc_id)
        title = trim_title(title)
        content = trim_content(content, clean_query)

        docs.append(
            {
                "path": f'Document {doc_id_disp[:-4]} - Collection {col_id}',
                "id": doc_id,
                "title": title.title(),
                "content": content,
            })
        f.close()

    return docs

def get_title_content(file, col_id, doc_id):
    title_ends = False
    title, content = '', ''

    for line in file:
        if line[:2] == '  ':
            title_ends = True
        if title_ends:
            content += line.strip()
        else:
            title += line.strip()
    
    if content == '':
        content = title
        title = f'Document {doc_id[:-4]} - Collection {col_id}'
    return title, content

def trim_title(title):
    title = re.sub(r"\d+. ", "", title)
    title = (title[:70]) if len(title) > 70 else title
    if title[-1] != ".":
        title += " ..."
    return title


def trim_content(raw_content, clean_query):
    content = []

    sentences = re.findall(r"([^.]*\.)", raw_content)
    for sentence in sentences:
        if all(word in sentence for word in clean_query):
            content.append(sentence)

    if not content:
        for q in clean_query:
            try:
                match = re.findall(r"([^.]*?" + q + "[^.]*\.)", raw_content)[0]
                content.append(match)
            except IndexError:
                continue

    if content:
        content = " ... ".join(content)
        for q in clean_query:
            content = content.replace(q, "<b>" + q + "</b>")
        content = "<p>" + content + "</p>"
    else:
        content = ""
    return content

def view_doc(request, pk):
    pk = int(pk)
    if pk < 1 or pk > 1033:
        return HttpResponseNotFound("Document not found")

    block = pk // 100 + 1
    path = os.path.join("main/engine", "collection", str(block), f"{pk}.txt")
    col_id, doc_id = path[12:].split('/')[1:]

    f = open(path)
    title, content = get_title_content(f, col_id, doc_id)

    context = {
        "pk": pk,
        "title": title.title(),
        "path": f'Document {doc_id[:-4]} - Collection {col_id}',
        "content": content,
    }
    return render(request, "doc.html", context=context)
