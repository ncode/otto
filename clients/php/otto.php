<?
class Otto {
    function Otto($url){
        $this->curl = curl_init();
        curl_setopt($this->curl, CURLOPT_URL, $url);
        curl_setopt($this->curl, CURLOPT_HEADER, 1); 
        curl_setopt($this->curl, CURLOPT_RETURNTRANSFER, 1); 
    }

    function get(){
        return curl_exec($this->curl);
    }

    function close(){
        curl_close($this->curl);
        return true;
    }

    function read(){
        return $this->get();
    }

    function write($filename){

        if !is_file($filename) return false;

        curl_setopt($this->curl, CURLOPT_PUT, true);
        curl_setopt($this->curl, CURLOPT_INFILESIZE, filesize($filename));
        curl_setopt($this->curl, CURLOPT_INFILE, $filename);
        curl_setopt($this->curl, CURLOPT_RETURNTRANSFER, );

        return true;
    }

    function delete($filename){
        return true;
    }
}

$teste = new Otto('http://');
echo $teste->get();


?>
