
function loadingDiv(){

        const html =  `<div class="row top-buffer">
             <div class="col-sm">
             </div>
             <div class = 'col-sm'>
                    <div class="alert alert-warning alert-dismissible fade show d-flex justify-content-between" role="alert">
                        <span> Em atualizacao! </span>
                        <button type="button" class="close" data-dismiss="alert" aria-label="Close">
                        </button>
                    </div>
             </div>
             <div class = 'col-sm'>
             </div>
         </div>
         <div class="row top-buffer low-bufferr">
             <div class="col-sm">
             </div>
             <div class = 'col-sm'>
                    <div class = "loader"> </div>
             </div>
             <div class = 'col-sm'>

             </div>
         </div>`;

    return html
};

function httpGet(theUrl)
{
    var xmlHttp = new XMLHttpRequest();
    xmlHttp.open( "GET", theUrl, false ); // false for synchronous request
    xmlHttp.send( null );
    return xmlHttp.responseText;
}

function getAtualizHttp(){

    const url=window.location.href + '/app_status';
    const r = httpGet(url);
    console.log(r);
    const status = JSON.parse(r);

    return status
};

function setar_div_loading(teste){

    const html = loadingDiv();
    const div = document.getElementById('alerta_atualiz');
    if(teste){
        div.innerHTML = html;
    }
    else{
        div.innerHTML = '';
    };

};

function check_atualiz()
    {
    console.log('rodei');
    const status = getAtualizHttp();
    console.log(status);
    console.log(status['Banco']['EmAtualizacao']);
    const teste = status['Banco']['EmAtualizacao'];
    setar_div_loading(teste);

    return teste;
};


async function check_atualiz_while(){

    while(check_atualiz()){
        await new Promise(r => setTimeout(r, 2000));
        console.log('esperando...')
    };
    console.log('terminou!')
};

function check_file_atualizada(tb_name){

   const status = getAtualizHttp();
   console.log(status);
   const file_mdata = status.Files[tb_name];
   console.log(file_mdata);
   if(!file_mdata.Criada | !file_mdata.Atualizada){
        return false;
   }else{
        return true;
   };
}


function bind_function(element){

    const tb_name = element;

    function file_alert(){

        console.log('entrou no alert');
        console.log(tb_name);
        if (!check_file_atualizada(tb_name)){
             window.alert("O arquivo está sendo gerado, aguarde um instante");
        }else{
            console.log('File esta ok');
            console.log(tb_name);
        };

    };

    return file_alert;
};

console.log('js correto2');

const bind_cremacao = bind_function('cremacao');
const bind_sepultamento = bind_function('sepultamento');
const bind_dados_obito = bind_function('dados_obito');
const bind_contratacao = bind_function('contratacao');


check_atualiz_while();
