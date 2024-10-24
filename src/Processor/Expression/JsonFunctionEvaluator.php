<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Hamcrest\Type\IsObject;
use Vimeo\MysqlEngine\FakePdoInterface;
use Vimeo\MysqlEngine\Processor\ProcessorException;
use Vimeo\MysqlEngine\Processor\QueryResult;
use Vimeo\MysqlEngine\Processor\Scope;
use Vimeo\MysqlEngine\Query\Expression\ColumnExpression;
use Vimeo\MysqlEngine\Query\Expression\ConstantExpression;
use Vimeo\MysqlEngine\Query\Expression\Expression;
use Vimeo\MysqlEngine\Query\Expression\FunctionExpression;
use Vimeo\MysqlEngine\Query\Expression\IntervalOperatorExpression;
use Vimeo\MysqlEngine\Schema\Column;
use Vimeo\MysqlEngine\TokenType;
use SimpleData\SimpleData;
use JsonSchema\Validator as JsonValidator;

final class JsonFunctionEvaluator
{
    /**
     * @param array<string, mixed> $row
     * @param array<string, Column> $columns
     *
     * @return mixed
     */
    public static function evaluate(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) {
        SimpleData::$PATH_SEPARATOR = '.';
        
        switch ($expr->functionName) {
            case 'JSON_ARRAY':
                return static::jsonArray($conn, $scope, $expr,$row, $result);
            case 'JSON_ARRAY_APPEND':
                return static::jsonArrayAppend($conn, $scope, $expr,$row, $result);
            case 'JSON_ARRAY_INSERT':
                return static::jsonArrayInsert($conn, $scope, $expr,$row, $result);
            case 'JSON_CONTAINS':
                return static::jsonContains($conn, $scope, $expr,$row, $result);
            case 'JSON_CONTAINS_PATH':
                return static::jsonContainsPath($conn, $scope, $expr,$row, $result);
            case 'JSON_DEPTH':
                return static::jsonDepth($conn, $scope, $expr,$row, $result);
            case 'JSON_EXTRACT':
                return static::jsonExtract($conn, $scope, $expr,$row, $result);
            case 'JSON_INSERT':
                return static::jsonInsert($conn, $scope, $expr,$row, $result);
            case 'JSON_KEYS':
                return static::jsonKeys($conn, $scope, $expr,$row, $result);
            case 'JSON_LENGTH':
                return static::jsonLength($conn, $scope, $expr,$row, $result);
            case 'JSON_MERGE':
            case 'JSON_MERGE_PRESERVE':
                return static::jsonMergePreserve($conn, $scope, $expr,$row, $result);
            case 'JSON_MERGE_PATCH':
                return static::jsonMergePatch($conn, $scope, $expr,$row, $result);
            case 'JSON_OBJECT':
                return static::jsonObject($conn, $scope, $expr,$row, $result);
            case 'JSON_OVERLAPS':
                return static::jsonOverlaps($conn, $scope, $expr,$row, $result);
            case 'JSON_PRETTY':
                return static::jsonPretty($conn, $scope, $expr,$row, $result);
            case 'JSON_QUOTE':
                return static::jsonQuote($conn, $scope, $expr,$row, $result);
            case 'JSON_REMOVE':
                return static::jsonRemove($conn, $scope, $expr,$row, $result);
            case 'JSON_REPLACE':
                return static::jsonReplace($conn, $scope, $expr,$row, $result);
            case 'JSON_SCHEMA_VALID':
                return static::jsonSchemaValid($conn, $scope, $expr,$row, $result);
            case 'JSON_SCHEMA_VALIDATION_REPORT':
                return static::jsonSchemaValidationReport($conn, $scope, $expr,$row, $result);
            case 'JSON_SEARCH':
                return static::jsonSearch($conn, $scope, $expr,$row, $result);
            case 'JSON_SET':
                return static::jsonSet($conn, $scope, $expr,$row, $result);
            case 'JSON_STORAGE_SIZE':
                return static::jsonStorageSize($conn, $scope, $expr,$row, $result);
            // case 'JSON_TABLE':
            //     return static::jsonInsert($conn, $scope, $expr,$row, $result);
            case 'JSON_TYPE':
                return static::jsonType($conn, $scope, $expr,$row, $result);
            case 'JSON_UNQUOTE':
                return static::jsonUnquote($conn, $scope, $expr,$row, $result);
            case 'JSON_VALID':
                return static::jsonValid($conn, $scope, $expr,$row, $result);
            // case 'JSON_VALUE':
            //     return static::jsonInsert($conn, $scope, $expr,$row, $result);
            // case 'MEMBER_OF':
            //     return static::jsonInsert($conn, $scope, $expr,$row, $result);
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    

                    

        }

       return new ProcessorException("Function " . $expr->functionName . " not implemented yet");
    }

    /**
     * Undocumented
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    protected static function jsonStorageSize($conn, $scope, $expr,$row, $result)
    {
        $params = $expr->args;
        if(!$params){
            throw new \Exception("JSON_STORAGE_SIZE expects 1 parameter");
        }

        $document = array_shift($params);
        $document = Evaluator::evaluate($conn, $scope, $document, $row, $result);

        if(!JsonOperatorEvaluator::validateJSON($document)){
            throw new \Exception("JSON_STORAGE_SIZE expects as valid json value");
        }


        return mb_strlen($document);
    }

    /**
     * Undocumented
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    protected static function jsonUnquote($conn, $scope, $expr,$row, $result)
    {
        $params = $expr->args;
        if(!$params){
            throw new \Exception("JSON_UNQUOTE expects 1 parameter");
        }

        $document = array_shift($params);
        $document = Evaluator::evaluate($conn, $scope, $document, $row, $result);

        if(!JsonOperatorEvaluator::validateJSON($document)){
            throw new \Exception("JSON_UNQUOTE expects as valid json value");
        }

        $value = json_decode($document);

        if(is_array($value) || is_object($value)){
            return json_encode($value);
        }

        return $value;
    }

    /**
     * Undocumented
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    protected static function jsonArrayAppend($conn, $scope, $expr,$row, $result)
    {
        $params = $expr->args;
        if(!$params){
            throw new \Exception("JSON_ARRAY_APPEND expects at least 3 parameters");
        }

        $document = array_shift($params);
        $document = Evaluator::evaluate($conn, $scope, $document, $row, $result);

        if(!JsonOperatorEvaluator::validateJSON($document)){
            throw new \Exception("JSON_ARRAY_APPEND expects as valid document as json");
        }
        
        if(count($params)%2 != 0){
            throw new \Exception("JSON_ARRAY_APPEND expects as arguments a path and value for each value to insert");
        }

        $params = array_chunk($params,2);

        $document   = json_decode($document);
        
        foreach($params as [$path,$value]){
            $path   = Evaluator::evaluate($conn, $scope, $path, $row, $result);
            $value  = Evaluator::evaluate($conn, $scope, $value, $row, $result);

            $path = static::sanitizePathForSimpleData($path);

            $item = simple_data($document);
            $target = $item->find($path);

            $targetValue = ($target)
                ?$target->raw()
                :[];
            
            if(!is_array($targetValue)){
                $targetValue = [$targetValue];
            }

            $targetValue[] = $value;

            $item->set($path,$targetValue);
        }

        return json_encode($item->raw());
    }


    /**
     * Undocumented
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    protected static function jsonArray($conn, $scope, $expr,$row, $result)
    {
        $params = $expr->args;
        if(!$params){
            throw new \Exception("JSON_ARRAY expects at least 1 parameters");
        }

        $document = [];

        foreach($params as $value){
            $value  = Evaluator::evaluate($conn, $scope, $value, $row, $result);

            $document[] = $value;
        }

        return json_encode($document);
    }

    /**
     * Undocumented
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    protected static function jsonArrayInsert($conn, $scope, $expr,$row, $result)
    {
        $params = $expr->args;
        if(!$params){
            throw new \Exception("JSON_ARRAY_INSERT expects at least 3 parameters");
        }

        $document = array_shift($params);
        $document = Evaluator::evaluate($conn, $scope, $document, $row, $result);

        if(!JsonOperatorEvaluator::validateJSON($document)){
            throw new \Exception("JSON_ARRAY_INSERT expects as valid document as json");
        }
        
        if(count($params)%2 != 0){
            throw new \Exception("JSON_ARRAY_INSERT expects as arguments a path and value for each value to insert");
        }

        $params = array_chunk($params,2);

        $document   = json_decode($document);
        
        $results = [];
        foreach($params as [$path,$value]){
            $path   = Evaluator::evaluate($conn, $scope, $path, $row, $result);
            $value  = Evaluator::evaluate($conn, $scope, $value, $row, $result);

            $path = static::sanitizePathForSimpleData($path);

            $item = simple_data($document);
            $target = $item->find($path);

            if(!$target || !$target->parent() || !$target->parent()->isArray()){
                $results[] = null;
                continue;
            }
            
            $realTarget = $target->parent();
            $childs     = $realTarget->items();
            $tmp        = [];
            $inserted   = false;


            foreach($childs as $key=>$child){
                if($key == $target->key() && !$inserted){
                    $tmp[] = $value;
                    $inserted = true;
                }

                $tmp[] = $child->raw();
            }

            if(!$inserted){
                $realTarget->append($value);
            }
            else{
                if($realTarget->parent()){
                    $realTarget->parent()->set($realTarget->key(),$tmp);
                }
                else{
                    $item = simple_data($tmp);
                }
            }

            $results[] = $item->raw();
        }

        return static::makeResultOnResultset($item,$results);
    }

    /**
     * Undocumented
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    protected static function jsonInsert($conn, $scope, $expr,$row, $result)
    {
        $params = $expr->args;
        if(!$params){
            throw new \Exception("JSON_INSERT expects at least 3 parameters");
        }

        $document = array_shift($params);
        $document = Evaluator::evaluate($conn, $scope, $document, $row, $result);

        if(!JsonOperatorEvaluator::validateJSON($document)){
            throw new \Exception("JSON_INSERT expects as valid document as json");
        }
        
        if(count($params)%2 != 0){
            throw new \Exception("JSON_INSERT expects as arguments a path and value for each value to insert");
        }

        $params     = array_chunk($params,2);
        $document   = json_decode($document);
        
        $results = [];
        foreach($params as [$path,$value]){
            $path   = Evaluator::evaluate($conn, $scope, $path, $row, $result);
            $value  = Evaluator::evaluate($conn, $scope, $value, $row, $result);

            $originalPath   = $path;
            $path           = static::sanitizePathForSimpleData($path);
            $item           = simple_data($document);

            $blocks = explode('.',$path);
            $key    = array_pop($blocks);
            $target = ($blocks)
                ?$item->find(implode('.',$blocks))
                :$item;

            if($target){
                if($target->isObject()){
                    if($target->has($key)){
                        $results[] = null;
                        continue;
                    }

                    $newValue = json_decode($value);
                    $target->set($key,$newValue);
                    $results[] = $item->raw();
                }
                else{
                    $expression = clone $expr;
                    $expression->args = [
                        [json_encode($document),0],
                        [$originalPath,0],
                        [$value,0],
                    ];

                    $fnResult = static::jsonArrayInsert($conn,$scope,$expression,$row,$result);
                    if($fnResult !== null){
                        $item = simple_data(json_decode($fnResult));
                    }

                    $results[] = $result;
                }
            }
            else{
                $results[] = null;
                continue;
            }
        }

        return static::makeResultOnResultset($item,$results);
    }


    /**
     * Undocumented
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    protected static function jsonContains($conn, $scope, $expr,$row, $result)
    {
        $params = $expr->args;
        if(count($params) < 2){
            throw new \Exception("JSON_CONTAINS expects at least 2 parameters");
        }

        $target = array_shift($params);
        $target = Evaluator::evaluate($conn, $scope, $target, $row, $result);

        if(!JsonOperatorEvaluator::validateJSON($target)){
            throw new \Exception("JSON_CONTAINS expects a valid seaching json");
        }
        
        $contained = array_shift($params);
        $contained = Evaluator::evaluate($conn, $scope, $contained, $row, $result);

        if(!JsonOperatorEvaluator::validateJSON($contained)){
            throw new \Exception("JSON_CONTAINS expects a valid seaching json");
        }

        $path = array_shift($params);
        if($path){
            $path = Evaluator::evaluate($conn, $scope, $path, $row, $result);
            if(!JsonOperatorEvaluator::validatePath($path)){
                throw new \Exception("JSON_CONTAINS expects a valid json path as 3th argument");
            }

            $path = static::sanitizePathForSimpleData($path);
        }

        $target = simple_data(json_decode($target));

        if($path){
            $target = $target->find($path);
        }
        
        $contained = simple_data(json_decode($contained));

        if(!$target){
            return 0;
        }


        return (int)(
            $target::TYPE == $contained::TYPE &&
            (json_encode($target->raw()) === json_encode($contained->raw()))
        );
    }

    /**
     * Undocumented
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    protected static function jsonContainsPath($conn, $scope, $expr,$row, $result)
    {
        $params = $expr->args;

        if(count($params) < 3){
            throw new \Exception("JSON_CONTAINS_PATH expects at least 3 parameters");
        }

        $document = array_shift($params);
        $document = Evaluator::evaluate($conn, $scope, $document, $row, $result);

        if(!JsonOperatorEvaluator::validateJSON($document)){
            throw new \Exception("JSON_CONTAINS_PATH expects a valid json");
        }
        
        $oneOrAll = array_shift($params);
        $oneOrAll = Evaluator::evaluate($conn, $scope, $oneOrAll, $row, $result);

        if(!in_array($oneOrAll,['one','all'])){
            throw new \Exception("JSON_CONTAINS_PATH expects a 'one' or 'all' as second argument");
        }

        $document   = simple_data(json_decode($document));
        $results    = [];
        foreach($params as $path){
            $path = Evaluator::evaluate($conn, $scope, $path, $row, $result);

            if(!JsonOperatorEvaluator::validatePath($path)){
                throw new \Exception("JSON_CONTAINS_PATH expects a valid json path");
            }

            $path = static::sanitizePathForSimpleData($path);
        
            if($document->find($path) !== null){
                if($oneOrAll == 'one'){
                    return 1;
                }

                $results[] = true;
            }
            else{
                $results[] = false;
            }            
        }

        return ($oneOrAll == 'all')
            ?((in_array(false,$results))
                ?0
                :1)
            :0;
    }

    /**
     * Undocumented
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    protected static function jsonDepth($conn, $scope, $expr,$row, $result)
    {
        $params = $expr->args;

        if(count($params) < 1){
            throw new \Exception("JSON_DEPTH expects 1 parameter");
        }

        $document = array_shift($params);
        $document = Evaluator::evaluate($conn, $scope, $document, $row, $result);

        if(!JsonOperatorEvaluator::validateJSON($document)){
            throw new \Exception("JSON_DEPTH expects a valid json");
        }

        $document = json_decode($document,true);

        if(is_null($document)){
            return null;
        }

        $maxDepth = 0;

        $calculateDepth = function($data, $currentDepth = 1) use (&$maxDepth, &$calculateDepth) {
            if (is_array($data)) {
                foreach ($data as $value) {
                    $calculateDepth($value, $currentDepth + 1);
                }
            }
            $maxDepth = max($maxDepth, $currentDepth);
        };

        $calculateDepth($document);
        

        return $maxDepth;
    }


    /**
     * Undocumented
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    protected static function jsonLength($conn, $scope, $expr,$row, $result)
    {
        $params = $expr->args;
        if(count($params) < 1){
            throw new \Exception("JSON_LENGTH expects at least 1 parameters");
        }

        $target = array_shift($params);
        $target = Evaluator::evaluate($conn, $scope, $target, $row, $result);

        if(!JsonOperatorEvaluator::validateJSON($target)){
            throw new \Exception("JSON_LENGTH expects a valid json document");
        }
        
        $path = array_shift($params);
        if($path){
            $path = Evaluator::evaluate($conn, $scope, $path, $row, $result);
            if(!JsonOperatorEvaluator::validatePath($path)){
                throw new \Exception("JSON_LENGTH expects a valid json path as 2th argument");
            }

            $path = static::sanitizePathForSimpleData($path);
        }

        $target = json_decode($target);
        if($target === null){
            return null;
        }
        elseif(is_scalar($target)){
            return 1;
        }
        else{
            $target = simple_data($target);
            if($path){
                $target = $target->find($path);
            }

            if($target === null){
                return null;
            }
            elseif($target->isValue()){
                return 1;
            }
            else{
                return count($target->items());
            }
        }
    }


    /**
     * Undocumented
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    protected static function jsonType($conn, $scope, $expr,$row, $result)
    {
        $params = $expr->args;

        if(count($params) < 1){
            throw new \Exception("JSON_TYPE expects 1 parameter");
        }

        $document = array_shift($params);
        $document = Evaluator::evaluate($conn, $scope, $document, $row, $result);

        if(!JsonOperatorEvaluator::validateJSON($document)){
            throw new \Exception("JSON_TYPE expects a valid json");
        }

        $document = json_decode($document);

        $type = gettype($document);
        
        $verifyTemporalType = function($input) {
            $input = trim($input);

            // DATE (YYYY-MM-DD)
            $dateRegex = '/^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$/';
        
            // TIME (HH:MM:SS)
            $timeRegex = '/^(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d$/';
        
            // DATETIME (YYYY-MM-DD HH:MM:SS) or TIMESTAMP (YYYY-MM-DDTHH:MM:SS)
            $dateTimeRegex = '/^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])[\sT](?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d$/';
        
            if (preg_match($dateTimeRegex, $input)) {
                return "DATETIME";
            } elseif (preg_match($dateRegex, $input)) {
                return "DATE";
            } elseif (preg_match($timeRegex, $input)) {
                return "TIME";
            } else {
                return false;
            }
        };

        switch($type){
            case !ctype_print($document):
                return 'BLOB';
            case $verifyTemporalType($document) == 'DATETIME':
                return 'DATETIME';
            case $verifyTemporalType($document) == 'DATE':
                return 'DATE';
            case $verifyTemporalType($document) == 'TIME':
                return 'TIME';
            case 'object':
            case 'array':
            case 'boolean':
            case 'NULL':
            case 'integer':
            case 'double':
            case 'string':
                return strtoupper($type);
            default:
                return 'OPAQUE';
        }
    }

    /**
     * Undocumented
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    protected static function jsonValid($conn, $scope, $expr,$row, $result)
    {
        $params = $expr->args;

        if(count($params) < 1){
            throw new \Exception("JSON_TYPE expects 1 parameter");
        }

        $document = array_shift($params);
        $document = Evaluator::evaluate($conn, $scope, $document, $row, $result);

        return (int)JsonOperatorEvaluator::validateJSON($document);
    }



    /**
     * Undocumented
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    protected static function jsonExtract($conn, $scope, $expr,$row, $result)
    {
        $params = $expr->args;
        if(count($params) < 2){
            throw new \Exception("JSON_EXTRACT expects at least 2 parameters");
        }

        $document = array_shift($params);
        $document = Evaluator::evaluate($conn, $scope, $document, $row, $result);

        if(!JsonOperatorEvaluator::validateJSON($document)){
            throw new \Exception("JSON_EXTRACT expects a valid seaching json");
        }

        if(count($params) == 0){
            throw new \Exception("JSON_EXTRACT expects at last 1 or more paths");
        }
        
        $document   = json_decode($document);
        $document   = simple_data($document);

        
        $results = [];
        foreach($params as $path){
            $path   = Evaluator::evaluate($conn, $scope, $path, $row, $result);

            if(!JsonOperatorEvaluator::validatePath($path)){
                throw new \Exception("JSON_EXTRACT expects avalid path");
            }

            $path = static::sanitizePathForSimpleData($path);

            if($document->isValue()){
                $results[] = null;
                continue;
            }


            $target = $document->find($path);

            $results[] = ($target !== null)
                ?$target->raw()
                :null;

        }

        $endingResult = count($params) == 1
            ?$results[0]
            :$results;

        return json_encode($endingResult);
    }


    /**
     * Undocumented
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    protected static function jsonKeys($conn, $scope, $expr,$row, $result)
    {
        $params = $expr->args;
        if(count($params) < 1){
            throw new \Exception("JSON_KEYS expects at least 1 parameters");
        }

        $target = array_shift($params);
        $target = Evaluator::evaluate($conn, $scope, $target, $row, $result);

        if(!JsonOperatorEvaluator::validateJSON($target)){
            throw new \Exception("JSON_KEYS expects a valid json document");
        }
        
        $path = array_shift($params);
        if($path){
            $path = Evaluator::evaluate($conn, $scope, $path, $row, $result);
            if(!JsonOperatorEvaluator::validatePath($path)){
                throw new \Exception("JSON_KEYS expects a valid json path as 2th argument");
            }

            $path = static::sanitizePathForSimpleData($path);
        }

        $target = simple_data(json_decode($target));

        if($target->isValue()){
            return null;
        }

        if($path){
            $target = $target->find($path);
        }

        if($target === null || !$target->isObject()){
            return null;
        }

        return json_encode($target->keys());
    }

    /**
     * Undocumented
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    protected static function jsonOverlaps($conn, $scope, $expr,$row, $result)
    {
        $params = $expr->args;
        if(count($params) < 2){
            throw new \Exception("JSON_OVERLAPS expects at least 2 parameters");
        }

        $target = array_shift($params);
        $target = Evaluator::evaluate($conn, $scope, $target, $row, $result);

        if(!JsonOperatorEvaluator::validateJSON($target)){
            throw new \Exception("JSON_OVERLAPS expects a valid json document");
        }
        
         

        $secondTarget = array_shift($params);
        $secondTarget = Evaluator::evaluate($conn, $scope, $secondTarget, $row, $result);

        if(!JsonOperatorEvaluator::validateJSON($secondTarget)){
            throw new \Exception("JSON_OVERLAPS expects a valid json document");
        }

        $jsonize = function(array $array){
            $out = [];
            foreach($array as $k=>$v){
                $out[$k] = json_encode($v);
            }

            return $out;
        };

        
        $target = simple_data(json_decode($target));
        $secondTarget = simple_data(json_decode($secondTarget));
        
        //VALUE vs VALUE
        if($target::TYPE == 'VALUE' && $secondTarget::TYPE == 'VALUE'){
            return (int)($target->raw() === $secondTarget->raw());
        }
        //VALUE vs ARRAY
        if($target::TYPE == 'VALUE' && $secondTarget::TYPE == 'ARRAY'){
            $target         = simple_data([$target->raw()]);
            $commonElements = array_intersect(
                $jsonize($target->raw()), 
                $jsonize($secondTarget->raw())
            );
            return (int)(count($commonElements) > 0);
        }
        //ARRAY vs VALUE
        if($target::TYPE == 'ARRAY' && $secondTarget::TYPE == 'VALUE'){
            $secondTarget   = simple_data([$secondTarget->raw()]);
            $commonElements = array_intersect(
                $jsonize($target->raw()), 
                $jsonize($secondTarget->raw())
            );
            return (int)(count($commonElements) > 0);
        }
        //ARRAY vs ARRAY
        if($target::TYPE == 'ARRAY' && $secondTarget::TYPE == 'ARRAY'){
            $commonElements = array_intersect(
                $jsonize($target->raw()), 
                $jsonize($secondTarget->raw())
            );
            return (int)(count($commonElements) > 0);
        }
        //OBJECT vs OBJECT
        if($target::TYPE == 'OBJECT' && $secondTarget::TYPE == 'OBJECT'){
            $target         = get_object_vars($target->raw());
            $secondTarget   = get_object_vars($secondTarget->raw());

            $commonElements = array_intersect_assoc(
                $jsonize($target), 
                $jsonize($secondTarget)
            );
            return (int)(count($commonElements) > 0);
        }
        else{
            //error?
            throw new \Exception("JSON_OVERLAPS can not compare a ".strtolower($target::TYPE)." with ".strtolower($target::TYPE));
        }

    }

    /**
     * Undocumented
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    protected static function jsonMergePreserve($conn, $scope, $expr,$row, $result)
    {
        $params = $expr->args;
        if(count($params) < 2){
            throw new \Exception("JSON_MERGE_PRESERVE expects at least 2 parameters");
        }

        $current = simple_data(null);

        foreach($params as $k=>$param){

            $newItem = Evaluator::evaluate($conn, $scope, $param, $row, $result);
            if(!JsonOperatorEvaluator::validateJSON($newItem)){
                throw new \Exception("JSON_MERGE_PRESERVE expects a valid json document");
            }

            $newItem    = simple_data(json_decode($newItem));


            if($k == 0){
                $current = $newItem;
            }
            else{
                switch($newItem){
                    case ($newItem->raw() === null):
                        return null;
                    break;
                    // VALUE vs VALUE
                    case $newItem::TYPE == 'VALUE' && $current::TYPE == 'VALUE':
                        $current = simple_data([$current->raw(),$newItem->raw()]);
                    break;
                    // VALUE vs ARRAY
                    case $newItem::TYPE == 'VALUE' && $current::TYPE == 'ARRAY':
                        $current = $current->append($newItem)->raw();
                    break;
                    // VALUE vs OBJECT
                    case $newItem::TYPE == 'VALUE' && $current::TYPE == 'OBJECT':
                        throw new \Exception("Can not merge a value into an object");
                    break;
                    // ARRAY vs VALUE
                    case $newItem::TYPE == 'ARRAY' && $current::TYPE == 'VALUE':
                        $current = $newItem->append($current);
                    break;
                    // ARRAY vs ARRAY
                    case $newItem::TYPE == 'ARRAY' && $current::TYPE == 'ARRAY':
                        $current = $current->append(...$newItem->raw());
                    break;
                    // ARRAY vs OBJECT
                    case $newItem::TYPE == 'ARRAY' && $current::TYPE == 'OBJECT':
                        $current = $newItem->append($current);
                    break;
                    // OBJECT vs VALUE
                    case $newItem::TYPE == 'OBJECT' && $current::TYPE == 'VALUE':
                        throw new \Exception("Can not merge a value into an object");
                    break;
                    // OBJECT vs ARRAY
                    case $newItem::TYPE == 'OBJECT' && $current::TYPE == 'ARRAY':
                        $current = simple_data([$newItem->raw(),...$current->raw()]);
                    break;
                    // OBJECT vs OBJECT
                    case $newItem::TYPE == 'OBJECT' && $current::TYPE == 'OBJECT':
                        $current = array_merge_recursive(
                            json_decode(json_encode($current->raw()),true), //convert to full array
                            json_decode(json_encode($newItem->raw()),true), //convert to full array
                        );
    
                        $current = simple_data(json_decode(json_encode($current))); //revert to real type
                    break;
                }
            }
        }

        return json_encode($current->raw());
    }


    /**
     * Undocumented
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    protected static function jsonMergePatch($conn, $scope, $expr,$row, $result)
    {
        $params = $expr->args;
        // dump($params);     

        if(count($params) < 2){
            throw new \Exception("JSON_MERGE_PRESERVE expects at least 2 parameters");
        }

        $current = simple_data(null);

        foreach($params as $k=>$param){

            $newItem = Evaluator::evaluate($conn, $scope, $param, $row, $result);
            if(!JsonOperatorEvaluator::validateJSON($newItem)){
                throw new \Exception("JSON_MERGE_PRESERVE expects a valid json document");
            }

            $newItem    = simple_data(json_decode($newItem));


            if($k == 0){
                $current = $newItem;
            }
            else{
                switch($newItem){
                    case ($newItem->raw() === null):
                        return json_encode(null);
                    break;
                    // VALUE vs VALUE
                    case $newItem::TYPE == 'VALUE' && $current::TYPE == 'VALUE':
                        $current = $newItem;
                    break;
                    // VALUE vs ARRAY
                    case $newItem::TYPE == 'VALUE' && $current::TYPE == 'ARRAY':
                        $current = $newItem;
                    break;
                    // VALUE vs OBJECT
                    case $newItem::TYPE == 'VALUE' && $current::TYPE == 'OBJECT':
                        $current = $newItem;
                    break;
                    // ARRAY vs VALUE
                    case $newItem::TYPE == 'ARRAY' && $current::TYPE == 'VALUE':
                        $current = $newItem;
                    break;
                    // ARRAY vs ARRAY
                    case $newItem::TYPE == 'ARRAY' && $current::TYPE == 'ARRAY':
                        $current = $newItem;
                    break;
                    // ARRAY vs OBJECT
                    case $newItem::TYPE == 'ARRAY' && $current::TYPE == 'OBJECT':
                        $current = $newItem;
                    break;
                    // OBJECT vs VALUE
                    case $newItem::TYPE == 'OBJECT' && $current::TYPE == 'VALUE':
                        $current = $newItem;
                    break;
                    // OBJECT vs ARRAY
                    case $newItem::TYPE == 'OBJECT' && $current::TYPE == 'ARRAY':
                        $current = $newItem;
                    break;
                    // OBJECT vs OBJECT
                    case $newItem::TYPE == 'OBJECT' && $current::TYPE == 'OBJECT':
                        $newCurrent = simple_data(new \StdClass);
                        
                        foreach($current->items() as $key=>$item){
                            $newCurrent->set($key,$item);
                        }
                        
                        foreach($newItem->items() as $newKey=>$item){
                            if($item->raw() === null && $newCurrent->get($newKey)){
                                $currentValue = $newCurrent->raw();
                                if($newCurrent->isObject()){
                                    unset($currentValue->{$newKey});
                                }
                                elseif($newCurrent->isArray()){
                                    unset($currentValue[$newKey]);
                                }
                                
                                $newCurrent->refresh($currentValue);
                            }
                            else{
                                if($newCurrent->has($newKey)){
                                    $expression = clone $expr;
                                    /**
                                     * @var ConstantExpression
                                     */
                                    $currentExpression = clone $param;
                                    $currentExpression->value = json_encode($newCurrent->get($newKey)->raw());
                                    /**
                                     * @var ConstantExpression
                                     */
                                    $newItemExpressison = clone $param;
                                    $newItemExpressison->value = json_encode($item->raw());
                                    $expression->args = [
                                        $currentExpression,
                                        $newItemExpressison
                                    ];
                                    // dump($expression);     
                                    $item = simple_data(json_decode(static::jsonMergePatch($conn,$scope,$expression,$row,$result)));
                                }

                                $newCurrent->set($newKey,$item);
                            }
                        }

                        $current = $newCurrent;
                    break;
                }
            }
        }

        return json_encode($current->raw());
    }


    /**
     * Undocumented
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    protected static function jsonObject($conn, $scope, $expr,$row, $result)
    {
        $params = $expr->args;
        if(!$params){
            throw new \Exception("JSON_OBJECT expects at least 2 parameters");
        }

        
        if(count($params)%2 != 0){
            throw new \Exception("JSON_OBJECT expects a pair key and value arguments for each property to create");
        }

        $params     = array_chunk($params,2);

        $object = new \stdClass;

        foreach($params as [$key,$value]){
            $newKey     = Evaluator::evaluate($conn, $scope, $key, $row, $result);
            $value      = Evaluator::evaluate($conn, $scope, $value, $row, $result);

            if($newKey === null){
                throw new \Exception("JSON_OBJECT expects a non NULL value fpr keys");
            }
            
            $object->{$newKey} = $value;
        }

        return json_encode($object);
    }

    /**
     * Undocumented
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    protected static function jsonPretty($conn, $scope, $expr,$row, $result)
    {
        $params = $expr->args;
        if(!$params){
            throw new \Exception("JSON_PRETTY expects 1 parameter");
        }

        $document = array_shift($params);

        $document= Evaluator::evaluate($conn, $scope, $document,$row, $result);
        if(!JsonOperatorEvaluator::validateJSON($document)){
            throw new \Exception("JSON_PRETTY expects a valid json document");
        }

        return json_encode(json_decode($document),JSON_PRETTY_PRINT);
    }

    /**
     * Undocumented
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    protected static function jsonQuote($conn, $scope, $expr,$row, $result)
    {
        $params = $expr->args;
        if(!$params){
            throw new \Exception("JSON_QUOTE expects 1 parameter");
        }

        $value = array_shift($params);


        return json_encode($value);
    }
    
    
    /**
     * Undocumented
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    protected static function jsonRemove($conn, $scope, $expr,$row, $result)
    {
        $params = $expr->args;
        if(count($params) < 2){
            throw new \Exception("JSON_REMOVE expects at least 2 parameters");
        }

        $document = array_shift($params);
        $document = Evaluator::evaluate($conn, $scope, $document, $row, $result);

        if(!JsonOperatorEvaluator::validateJSON($document)){
            throw new \Exception("JSON_REMOVE expects as valid document as json");
        }
        
        $document = simple_data(json_decode($document));
        
        foreach($params as $path){
            $path   = Evaluator::evaluate($conn, $scope, $path, $row, $result);
            if(!JsonOperatorEvaluator::validatePath($path)){
                throw new \Exception("JSON_REMOVE expects an valid path for working on");
            }

            $path   = static::sanitizePathForSimpleData($path);
            $target = $document->find($path);

            if($target){
                $target->remove();
            }
        }

        return json_encode($document->raw());
    }



    /**
     * Undocumented
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    protected static function jsonReplace($conn, $scope, $expr,$row, $result)
    {
        $params = $expr->args;
        if(!$params){
            throw new \Exception("JSON_REPLACE expects at least 3 parameters");
        }

        $document = array_shift($params);
        $document = Evaluator::evaluate($conn, $scope, $document, $row, $result);

        if(!JsonOperatorEvaluator::validateJSON($document)){
            throw new \Exception("JSON_REPLACE expects as valid document as json");
        }
        
        if(count($params)%2 != 0){
            throw new \Exception("JSON_REPLACE expects as arguments a path and value for each value to replace");
        }

        $params     = array_chunk($params,2);
        $document   = json_decode($document);
        
        $results = [];
        foreach($params as [$path,$value]){
            $path   = Evaluator::evaluate($conn, $scope, $path, $row, $result);
            $value  = Evaluator::evaluate($conn, $scope, $value, $row, $result);

            $originalPath   = $path;
            $path           = static::sanitizePathForSimpleData($path);
            $item           = simple_data($document);

            $blocks = explode('.',$path);
            $key    = array_pop($blocks);
            $target = ($blocks)
                ?$item->find(implode('.',$blocks))
                :$item;

            if(!$target){
                $results[] = null;
                continue;
            }
            else{
                if($target->isObject()){
                    if(!$target->has($key)){
                        $results[] = null;
                        continue;
                    }

                    $newValue = json_decode($value);
                    $target->set($key,$newValue);
                    $results[] = $item->raw();
                }
                else{
                    $expression = clone $expr;
                    $expression->args = [
                        [json_encode($document),0],
                        [$originalPath,0],
                        [$value,0],
                    ];

                    $fnResult = static::jsonArrayInsert($conn,$scope,$expression,$row,$result);
                    if($fnResult !== null){
                        $item = simple_data(json_decode($fnResult));
                    }

                    $results[] = $result;
                }
            }
        }

        return static::makeResultOnResultset($item,$results);
    }


    /**
     * Undocumented
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    public static function jsonSchemaValid($conn, $scope, $expr,$row, $result)
    {
        $params = $expr->args;
        if(count($params) < 2){
            throw new \Exception("JSON_SCHEMA_VALID expects 2 parameters");
        }

        $schema = array_shift($params);
        $schema = Evaluator::evaluate($conn, $scope, $schema, $row, $result);
        
        $document = array_shift($params);
        $document = Evaluator::evaluate($conn, $scope, $document, $row, $result);

        if(!JsonOperatorEvaluator::validateJSON($schema)){
            throw new \Exception("JSON_SCHEMA_VALID expects as valid json schema to use");
        }
        
        if(!JsonOperatorEvaluator::validateJSON($document)){
            throw new \Exception("JSON_REPLACE expects as valid document as json");
        }

        // dd(json_decode($document),json_decode($schema));
        $document   = json_decode($document);
        $schema     = json_decode($schema);
        $validator  = new JsonValidator;
        $validator->validate($document,$schema);

        return (int)$validator->isValid();
    }

    /**
     * Undocumented
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    public static function jsonSchemaValidationReport($conn, $scope, $expr,$row, $result)
    {
        $params = $expr->args;
        if(count($params) < 2){
            throw new \Exception("JSON_SCHEMA_VALIDATION_REPORT expects 2 parameters");
        }

        $schema = array_shift($params);
        $schema = Evaluator::evaluate($conn, $scope, $schema, $row, $result);
        
        $document = array_shift($params);
        $document = Evaluator::evaluate($conn, $scope, $document, $row, $result);

        if(!JsonOperatorEvaluator::validateJSON($schema)){
            throw new \Exception("JSON_SCHEMA_VALIDATION_REPORT expects as valid json schema to use");
        }
        
        if(!JsonOperatorEvaluator::validateJSON($document)){
            throw new \Exception("JSON_REPLACE expects as valid document as json");
        }

        // dd(json_decode($document),json_decode($schema));
        $document   = json_decode($document);
        $schema     = json_decode($schema);
        $validator  = new JsonValidator;
        $validator->validate($document,$schema);


        $result = ['valid'=>(bool)$validator->isValid()];
        if(!$validator->isValid()){
            foreach ($validator->getErrors() as $error) {
                $result['reason'] = $error['message'];
                // $result['schema-location'] = null;
                $result['document-location'] = '#'.$error['pointer'];
                $result['schema-failed-keyword'] = $error['property'];
                break;
            }            
        }
        return json_encode($result);
    }


    /**
     * Undocumented
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    public static function jsonSearch($conn, $scope, $expr,$row, $result)
    {
        $params = $expr->args;
        if(count($params) < 3){
            throw new \Exception("JSON_SEARCH expects 3 parameters");
        }

        $document = array_shift($params);
        $document = Evaluator::evaluate($conn, $scope, $document, $row, $result);

        if(!JsonOperatorEvaluator::validateJSON($document)){
            throw new \Exception("JSON_REPLACE expects as valid document as json");
        }

        if(is_null($document)){
            return null;
        }


        $oneOrAll = array_shift($params);
        $oneOrAll = Evaluator::evaluate($conn, $scope, $oneOrAll, $row, $result);
        if(!in_array($oneOrAll,['one','all'])){
            throw new \Exception("JSON_SEARCH expects 'one' or 'all' as a search mode");
        }
        
        $searchStr = array_shift($params);
        $searchStr = Evaluator::evaluate($conn, $scope, $searchStr, $row, $result);
        if(is_null($searchStr)){
            return null;
        }

        $escapeChar = array_shift($params);
        if($escapeChar){
            $escapeChar = Evaluator::evaluate($conn, $scope, $escapeChar, $row, $result);
            $escapeChar = $escapeChar ?? '\\';
        }
        else{
            $escapeChar = '\\';
        }


        $target     = simple_data(json_decode($document));
        $sqlValue   = ($escapeChar !== '')
            ?preg_replace('#(?<!'.preg_quote($escapeChar,'#').')%#Usmi', '*', $searchStr)
            :str_replace('%','*',$searchStr);

        if(!$params){
            $results =  $target->search($sqlValue);
        }
        else{
            $results = [];
            foreach($params as $path){
                $searchPath = Evaluator::evaluate($conn, $scope, $path, $row, $result);
                if(!JsonOperatorEvaluator::validatePath($searchPath)){
                    throw new \Exception("JSON_SEARCH expects a valid path into search.  {$searchPath} is given");
                }

                $searchPath = static::sanitizePathForSimpleData($searchPath);
              
                $target = $target->find($searchPath);

                if($target->isValue()){
                    if($target->match($sqlValue)){
                        $results[] = $target;
                    }
                }
                else{
                    foreach($target->search($sqlValue) as $foundItem){
                        $results[] = $foundItem;
                    }
                }
            }

        }

        if($results){
            $results = array_map(function($result){
                return JsonFunctionEvaluator::convertSimpleDataPathForSql($result);
            },$results);
        }
        else{
            return json_encode(null);
        }

        if($oneOrAll == 'one'){
            return  json_encode(array_shift($results));
        }

        return json_encode(array_values($results));
    }

    /**
     * Undocumented
     *
     * @param \Vimeo\MysqlEngine\FakePdoInterface $conn
     * @param \Vimeo\MysqlEngine\Processor\Scope $scope
     * @param \Vimeo\MysqlEngine\Query\Expression\FunctionExpression $expr
     * @param array $row
     * @param \Vimeo\MysqlEngine\Processor\QueryResult $result
     * @return string
     */
    public static function jsonSet($conn, $scope, $expr,$row, $result)
    {
        $params = $expr->args;
        if(count($params) < 3){
            throw new \Exception("JSON_SET expects 3 parameters");
        }

        $document = array_shift($params);
        $document = Evaluator::evaluate($conn, $scope, $document, $row, $result);

        if(!JsonOperatorEvaluator::validateJSON($document)){
            throw new \Exception("JSON_SET expects as valid document as json");
        }

        if(is_null($document)){
            return null;
        }



        if(count($params)%2 != 0){
            throw new \Exception("JSON_SET expects as arguments a path and value for each value to add or replace");
        }

        $params = array_chunk($params,2);

        $document   = json_decode($document);
        $document   = simple_data($document);

        foreach($params as [$path,$value]){
            $path   = Evaluator::evaluate($conn, $scope, $path, $row, $result);
            $value  = Evaluator::evaluate($conn, $scope, $value, $row, $result);


            if(!JsonOperatorEvaluator::validatePath($path)){
                throw new \Exception("JSON_SET expects as valid path to set or replace a value");
            }


            if(!JsonOperatorEvaluator::validateJSON($value)){
                throw new \Exception("JSON_SET expects as valid value to set");
            }
    

            $path = static::sanitizePathForSimpleData($path);
            $value = json_decode($value);

            $blocks     = explode(SimpleData::$PATH_SEPARATOR,$path);
            $key        = array_pop($blocks);
            $parentPath = implode(SimpleData::$PATH_SEPARATOR,$blocks);
        
            if($parentPath == ''){
                $parent = $document;
            }
            else{
                $parent = $document->find($parentPath);
            }            
            
            if($parent->isValue()){
                continue;
            }

            if(!$parent->has($key)){
                if($parent->isArray()){
                    $parent->append($value);
                }
                else{
                    $parent->set($key,$value);
                }
            }
            else{
                $parent->set($key,$value);
            }            
        }

        return json_encode($document->raw());
    }




    /**
     * makes the path compatible with simple_data
     *
     * @param string $path
     * @return string
     */
    public static function sanitizePathForSimpleData(string $path,bool $useWildCards = false)
    {
        $path = preg_replace('#^\$(.|\[)#','',trim($path));
            
        $path = str_replace('[','.',$path);
        $path = str_replace(']','',$path);

        if($path == '$'){
            return '*';
        }

        return $path; 
    }

    protected static function convertSimpleDataPathForSql(object $simpleDataObj)
    {
        $elements   = [];
        $item       = $simpleDataObj;
        while($item){
            if($item->parent()){
                if($item->parent()->isArray()){
                    array_unshift($elements,"[{$item->key()}]");
                }
                else{
                    array_unshift($elements,".{$item->key()}");
                }
            }

            $item = $item->parent();
        }

        array_unshift($elements,"$");

        return implode("",$elements);
    }



    /**
     * Undocumented function
     *
     * @param \simple_data\ArrayWalker|\simple_data\ObjectWalker|\simple_data\Value $jsonResult
     * @param array $resultSet
     * @return string|null
     */
    protected static function makeResultOnResultset($jsonResult,array $resultSet = [])
    {
        $endingResult = null;
        foreach($resultSet as $result){
            if($result !== null){
                $endingResult = $result;
            }
        }

        return ($endingResult === null)
            ?$endingResult
            :json_encode($jsonResult->raw());
    }
}
