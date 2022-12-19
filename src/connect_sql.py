"""Connect to MySQL database server"""
from mysql.connector import connect, Error
from omegaconf import OmegaConf


# Config 
conf = OmegaConf.load('../database.yaml')
