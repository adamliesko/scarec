import sys

def api_key_handler():
    """
    Writes the API key to api_key.txt file. It will create the file if it doesn't exist.
    This function is intended to be called from the Python command line using: python alchemyapi YOUR_API_KEY
    If you don't have an API key yet, register for one at: http://www.alchemyapi.com/api/register.html

    INPUT:
    argv[1] -> Your API key from AlchemyAPI. Should be 40 hex characters

    OUTPUT:
    none
    """
    if len(sys.argv[1]) == 40:
        # write the key to the file
        f = open('api_key.txt', 'w')
        f.write(sys.argv[1])
        f.close()
        print('Key: ' + sys.argv[1] + ' was written to api_key.txt')
        print('You are now ready to start using AlchemyAPI. For an example, run: python example.py')


if __name__ == '__main__':
    if len(sys.argv) == 2 and sys.argv[1]:
        api_key_handler()
    else:
        print('The key appears to invalid. Please make sure to use the 40 character key assigned by AlchemyAPI')