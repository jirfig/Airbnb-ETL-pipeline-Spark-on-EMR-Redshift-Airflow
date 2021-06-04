# syntax=docker/dockerfile:1

FROM jupyter/minimal-notebook:lab-3.0.14

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

ENV JUPYTER_ENABLE_LAB=yes
